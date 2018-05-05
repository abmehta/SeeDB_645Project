import csv
import psycopg2
import numpy as np
from scipy.stats import entropy

import matplotlib.pyplot as plt

from utils import *


aggregation_functions = ['avg', 'sum', 'max', 'min', 'count']
group_by_columns = [
    'workclass',
    'education',
    'occupation',
    'relationship',
    'race',
    'sex',
    'native_country',
    'salary'
]
measure_columns = [
    'age',
    'fnlwgt',
    'education_num',
    'capital_gain',
    'capital_loss',
    'hours_per_week'
]

#target = ['Married-civ-spouse','Married-spouse-absent','Marries-AF-spouse']
# Let reference be complement on target.


def create_adult_table(conn, cur):
    #Create table Adult
    cur.execute("DROP TABLE IF EXISTS Adult CASCADE;")
    cur.execute("""
                CREATE TABLE Adult (
                             age INT,
                             workclass VARCHAR(50),
                             fnlwgt INT,
                             education VARCHAR(50),
                             education_num INT,
                             marital_status VARCHAR(50),
                             occupation VARCHAR(50),
                             relationship VARCHAR(50),
                             race VARCHAR(50),
                             sex VARCHAR(50),
                             capital_gain INT,
                             capital_loss INT,
                             hours_per_week INT,
                             native_country VARCHAR(50),
                             salary VARCHAR(50));
                """)

    #Fill in table from csv file
    with open('adult.csv','r') as f:
       cur.copy_from(f,'adult',sep=',')

    #Add primary key
    cur.execute("alter table adult add column id serial primary key;")

    conn.commit()


def create_ref_tgt_views(conn, cur):
    #Create Target table
    cur.execute("DROP VIEW IF EXISTS Target;")
    cur.execute("""
        CREATE VIEW Target AS
        SELECT * FROM Adult
        WHERE to_tsvector('english',marital_status) @@ to_tsquery('english','married & !never-married');
    """)
    #Create Reference table (Complement of Target)
    cur.execute("DROP VIEW IF EXISTS Reference;")
    cur.execute("""
        CREATE VIEW Reference AS
        SELECT * FROM Adult
        WHERE to_tsvector('english',marital_status) @@ to_tsquery('english','!married | never-married');
    """)

    conn.commit()

def naive_search(cur, list_of_views, limits=None, top_k=5, measure='kld', verbose=False):
    '''
    Exhaustively iterate over all combinations of triplets and query the database
    '''

    grouped_views = group_views_by_grouping_column(list_of_views)

    query = "select {group}, {func}({m_col}) from {table} {where} group by {group};"

    if limits:
        where_clause = "where id >= {} and id < {}".format(limits[0], limits[1])
    else:
        where_clause = ""


    results = list()
    for group, views in grouped_views.items():
        for view in views:
            _, func, m_col = view
            target_query = query.format(group=group,
                                        func=func,
                                        m_col=m_col,
                                        table='target',
                                        where=where_clause)

            cur.execute(target_query)
            target_results = cur.fetchall()

            reference_query = query.format(group=group,
                                           func=func,
                                           m_col=m_col,
                                           table='reference',
                                           where=where_clause)
            cur.execute(reference_query)
            reference_results = cur.fetchall()

            dist = distance(target_results, reference_results)
            results.append((view, dist))

            if verbose:
                print "({}, {}, {})".format(group, func, m_col), dist

    return list(reversed(sorted(results, key=lambda x: x[1])))[:top_k]


def sharing_based_search(cur, list_of_views, limits=None, top_k=5, measure='kld', verbose=False):
    '''
    Apply Sharing optimizations by grouping multiple aggrerations within the same query
    '''

    grouped_views = group_views_by_grouping_column(list_of_views)

    query = "select {group} {aggregated_measures} from {table} {where} group by {group};"

    if limits:
        where_clause = "where id >= {} and id < {}".format(limits[0], limits[1])
    else:
        where_clause = ""


    results = list()
    for group, views in grouped_views.items():

        q_string = "".join(", {func}({m_col})".format(func=func, m_col=m_col) for _, func, m_col in views)

        target_query = query.format(group=group,
                                    aggregated_measures=q_string,
                                    table='target',
                                    where=where_clause)
        cur.execute(target_query)
        t_v = cur.fetchall()

        reference_query = query.format(group=group,
                                       aggregated_measures=q_string,
                                       table='reference',
                                       where=where_clause)
        cur.execute(reference_query)
        r_v = cur.fetchall()

        for i, view in enumerate(views):
            target_results = [(v[0], v[i+1]) for v in t_v]
            reference_results = [(v[0], v[i+1]) for v in r_v]

            dist = distance(target_results, reference_results, measure=measure)
            if verbose:
                print view, dist
            results.append((view, dist))


    return list(reversed(sorted(results, key=lambda x: x[1])))[:top_k]

def pruning_based_search(cur, list_of_views, search_method, num_partitions=15, top_k=5, measure='kld', verbose=False):
    '''
    Apply pruning optimizations based on Confidence Intervals derived from Hoeffding-Serfling inequality
    '''

    cur.execute('select max(id) from adult;')
    max_id = cur.fetchall()[0][0]

    partition_size = 1 + max_id / num_partitions

    current_views = list_of_views[:]

    mean_estimated_utility = dict()

    for i in range(num_partitions):
        l_bound = i*partition_size
        u_bound = l_bound + partition_size

        # this returns the results in sorted order!
        results = search_method(cur, current_views, limits=(l_bound, u_bound), top_k=None, measure=measure)

        for view, utility in results:
            prev_mean = mean_estimated_utility.get(view, 0.0)
            mean_estimated_utility[view] = (i * prev_mean + utility) / (i + 1)

        sorted_utilities = sorted([mean_estimated_utility[view] for view in current_views])

        max_utility = sorted_utilities[-1]
        kth_utility = sorted_utilities[-top_k] / max_utility

        if i == 0:
            if verbose:
                print "We dont want to cull anything on the first iteration because epsilon is NaN"
            continue


        epsilon_m = hoeffding_serfling_interval(i+1, num_partitions, 0.1)
        not_pruned_views = list()

        for view in current_views:
            est_util = mean_estimated_utility[view] / max_utility
            if (est_util + epsilon_m) >= (kth_utility - epsilon_m):
                not_pruned_views.append(view)

        culled = len(current_views) - len(not_pruned_views)

        if verbose:
            print "On iterated %d we pruned %d views." % (i, culled)

        current_views = not_pruned_views[:]

    if verbose:
        print "we finished with %d views." % len(current_views)


    return search_method(cur, current_views, top_k=top_k, measure=measure)




if __name__ == "__main__":
    connection = psycopg2.connect("dbname=census")
    cursor = connection.cursor()

    #create_adult_table(connection, cursor)
    #create_ref_tgt_views(connection, cursor)
    #connection.commit()


    init_list = create_initial_list_of_views(group_by_columns, measure_columns, aggregation_functions)

    measure = 'kld'


    for i in range(1):
        top_5 = pruning_based_search(cursor, init_list, sharing_based_search, measure=measure, verbose=True)

        #top_5 = sharing_based_search(cursor, init_list, verbose=False, measure=measure)

    for view, utility in top_5:
        print view, utility

        cursor.execute("select {g}, {f}({m}) from target group by {g};".format(g=view[0], f=view[1], m=view[2]))
        target = dict(cursor.fetchall())

        cursor.execute("select {g}, {f}({m}) from reference group by {g};".format(g=view[0], f=view[1], m=view[2]))
        reference = dict(cursor.fetchall())

        t = list()
        r = list()
        names = list()

        # we need this because this ensures that there are the same number of categories in each distribution
        for key in set().union(target.keys(), reference.keys()):
            names.append(key)
            t_val = float(target.get(key, 0))
            r_val = float(reference.get(key, 0))
            t.append(t_val)
            r.append(r_val)

        fig, ax = plt.subplots()

        width=0.35
        index = np.arange(len(names))

        t_bar = ax.bar(index, t, width, color='r')

        r_bar = ax.bar(index+width, r, width, color='b')

        ax.set_title("{f}({m}) grouped by {g} ({measure} = {util})".format(f=view[1],
                                                                           m=view[2],
                                                                           g=view[0],
                                                                           measure=measure,
                                                                           util=utility))
        ax.set_ylabel("{f}({m})".format(f=view[1], m=view[2]))
        ax.set_xticks(index + width/2)
        ax.set_xticklabels(names)
        ax.legend((t_bar[0], r_bar[0]), ('Married', 'Unmarried'))

    plt.show()




    connection.commit() # close the transaction on the database
    cursor.close()
    connection.close()
