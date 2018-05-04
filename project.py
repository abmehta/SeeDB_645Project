import csv
import psycopg2
import numpy as np
from scipy.stats import entropy

from utils import distance


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

def naive_search(grouping_columns, measure_columns, aggregation_functions, top_k=5, measure='kld', verbose=True):

    query = "select {group}, {func}({m_col}) from {table} group by {group};"
    results = list()
    for group in group_by_columns:
        for m_col in measure_columns:
            for func in aggregation_functions:
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
                results.append(((group, func, m_col), dist))

                if verbose:
                    print "({}, {}, {})".format(group, func, m_col), dist

    return list(reversed(sorted(results, key=lambda x: x[1])))[:top_k]


if __name__ == "__main__":
    conn = psycopg2.connect("dbname=census")
    cur = conn.cursor()

    create_adult_table(conn, cur)
    create_ref_tgt_views(conn, cur)
    conn.commit()


    naive_search(group_by_columns, measure_columns, aggregation_functions)

    cur.close()
    conn.close()
