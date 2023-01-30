import gc

import parseDicNew,parseTab
import psycopg2
from sqlalchemy import create_engine

conn_string = 'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/testdb'

db = create_engine(conn_string)
conn=db.connect()
conn1 = psycopg2.connect(user="postgres",
                              password="124kosm21",
                              host="127.0.0.1",
                              port="5432",
                              database="testdb")
print(conn)
print(conn1)

conn1.autocommit = True
cursor = conn1.cursor()
#['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']
for rinok in [['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']]:
    print(rinok)
    ss=parseDicNew.c_parseDic('final_5_2',rinok[0],rinok[1])
    ss.startParse()
    df_list=ss.df.returtDfs()
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

    ss=parseTab.c_parseFormula('final_5_2',rinok[0],rinok[1])
    ss.startParse()
    df_list=ss.df.returtDfs()
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

ss=parseDicNew.c_parseDic('final_5_2','udr\\dim','dim')
ss.startParse()
df_list=ss.df.returtDfs()

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

ss=parseDicNew.c_parseDic('final_5_2','udr\\dom','mem')
ss.startParse()
df_list=ss.df.returtDfs()
print(df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)

conn1.commit()
conn1.close()
