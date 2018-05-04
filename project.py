import csv
import psycopg2
import numpy as np
from scipy.stats import entropy

from utils import *


aggregation_functions = ['sum','max', 'min', 'avg', 'count']
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
    #Sample Query for Target
    cur.execute("""
        CREATE VIEW Reference AS
        SELECT * FROM Adult
        WHERE to_tsvector('english',marital_status) @@ to_tsquery('english','!married | never-married');
    """)

    conn.commit()

def find_kld_sex_age(cur):
    cur.execute("select sex, avg(age) from target group by sex;")
    tgt = cur.fetchall()
    #Sample Query for Reference
    cur.execute("select sex, avg(age) from reference group by sex;")
    ref = cur.fetchall()
    #K-L Divergence value
    print "KLD:", distance(tgt, ref)
    print "EMD:", distance(tgt, ref, measure='emd')

def naive_search(list_of_views, top_k=5, measure='kld', verbose=True):

    grouped_views = group_views_by_grouping_column(list_of_views)

    query = "select {group}, {func}({m_col}) from {table} where id < 5000 group by {group};"
    results = list()
    for group, views in grouped_views.items():
        for view in views:
            _, func, m_col = view
            target_query = query.format(group=group,
                                        func=func,
                                        m_col=m_col,
                                        table='target')

            cur.execute(target_query)
            target_results = cur.fetchall()

            reference_query = query.format(group=group,
                                           func=func,
                                           m_col=m_col,
                                           table='reference')
            cur.execute(reference_query)
            reference_results = cur.fetchall()

            dist = distance(target_results, reference_results)
            results.append((view, dist))

            if verbose:
                print "({}, {}, {})".format(group, func, m_col), dist

    return list(reversed(sorted(results, key=lambda x: x[1])))[:top_k]


def sharing_based_search(list_of_views, top_k=5, measure='kld', verbose=True):

    grouped_views = group_views_by_grouping_column(list_of_views)

    query = "select {group} {aggregated_measures} from {table}  group by {group};"

    results = list()
    for group, views in grouped_views.items():

        q_string = "".join(", {func}({m_col})".format(func=func, m_col=m_col) for _, func, m_col in views)

        target_query = query.format(group=group, aggregated_measures=q_string, table='target')
        cur.execute(target_query)
        t_v = cur.fetchall()

        reference_query = query.format(group=group, aggregated_measures=q_string, table='reference')
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

if __name__ == "__main__":
    conn = psycopg2.connect("dbname=census")
    cur = conn.cursor()

    #create_adult_table(conn, cur)
    #create_ref_tgt_views(conn, cur)
    #conn.commit()


    init_list = create_initial_list_of_views(group_by_columns, measure_columns, aggregation_functions)
    top_5_kld_sharing = sharing_based_search(init_list, verbose=True, measure='emd')

    for view in top_5_kld_sharing:
        print view


    cur.close()
    conn.close()
