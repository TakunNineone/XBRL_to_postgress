import gc
import datetime
import os

import parseDicNew,parseTab,parseMetaInf,parseIFRS_FULL
import psycopg2
from sqlalchemy import create_engine

print('begin',datetime.datetime.now())
conn_string = 'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/taxonomy_db'

db = create_engine(conn_string)
conn=db.connect()
conn1 = psycopg2.connect(user="postgres",
                              password="124kosm21",
                              host="127.0.0.1",
                              port="5432",
                              database="taxonomy_db")
print(conn)
print(conn1)

version='final_5_2'
period='2023-03-31'


ss=parseTab.c_parseTab(version,'bfo','bfo',period)
df_list=ss.startParse()
print(df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

conn1.commit()
conn1.close()
conn.close()
print('end',datetime.datetime.now())