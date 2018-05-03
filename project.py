#xfrom __future__ import divide
import csv
import psycopg2
import numpy as np
from scipy.stats import entropy

f = ['count','max', 'min', 'avg', 'count']
a = []
m = []
#target = ['Married-civ-spouse','Married-spouse-absent','Marries-AF-spouse']
# Let reference be complement on target.


def kldiv(tgt,ref):
   tgt_val = np.asarray([float(x[1]) for x in tgt])
   ref_val = np.asarray([float(x[1]) for x in ref])
   tgt_val /= np.sum(tgt_val)
   ref_val /= np.sum(ref_val)
   return entropy(tgt_val,ref_val)



conn = psycopg2.connect("dbname=census")
cur = conn.cursor()
#Create table Adult
cur.execute("DROP TABLE IF EXISTS Adult;")
cur.execute("CREATE TABLE Adult (age INT, workclass VARCHAR(50), fnlwgt INT, education VARCHAR(50), education_num INT, marital_status VARCHAR(50), occupation VARCHAR(50), relationship VARCHAR(50), race VARCHAR(50), sex VARCHAR(50), capital_gain INT, capital_loss INT, hours_per_week INT, native_country VARCHAR(50), salary VARCHAR(50));")
#Fill in table from csv file
with open('adult.csv','r') as f:
   cur.copy_from(f,'adult',sep=',')
#Create Target table
cur.execute("DROP TABLE IF EXISTS Target;")
cur.execute("CREATE TABLE Target AS SELECT * FROM Adult WHERE to_tsvector('english',marital_status) @@ to_tsquery('english','married & !never-married');")
#Create Reference table (Complement of Target)
cur.execute("DROP TABLE IF EXISTS Reference;")
#Sample Query for Target
cur.execute("CREATE TABLE Reference AS SELECT * FROM Adult WHERE to_tsvector('english',marital_status) @@ to_tsquery('english','!married | never-married');")
cur.execute("select sex,count(age) from target group by sex;")
tgt = cur.fetchall()
#Sample Query for Reference
cur.execute("select sex,count(age) from reference group by sex;")
ref = cur.fetchall()
#K-L Divergence value
print(kldiv(tgt,ref))
conn.commit()
cur.close()
conn.close()


