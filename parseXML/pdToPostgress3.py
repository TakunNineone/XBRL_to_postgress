import gc
import datetime
import parseDicNew, parseTab, parseMetaInf, parseIFRS_FULL
import psycopg2
from sqlalchemy import create_engine

print('begin', datetime.datetime.now())
conn_string = 'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/final_6_5'

db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(user="postgres",
                         password="124kosm21",
                         host="127.0.0.1",
                         port="5432",
                         database="final_6_5")
print(conn)
print(conn1)

version = 'final_6_5'
period = '2024-11-01'

# conn1.autocommit = True
cursor = conn1.cursor()
for rinok in [['nfo', 'nfo']]:

    print('parseDic', rinok)
    ss2 = parseDicNew.c_parseDic(version, rinok[0], rinok[1])
    df_list2 = ss2.startParse()
    print(df_list2.keys())
    str_headers = ''
    for xx in df_list2.keys():
        headers = [xx.strip() + ' VARCHAR, ' for xx in df_list2.get(xx).keys().values]
        for hh in headers:
            str_headers = str_headers + hh + '\n'
        str_headers = str_headers.strip()[:-1]
        df_list2.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
    del df_list2, ss2
    gc.collect()

conn1.commit()
conn1.close()
conn.close()
print('end', datetime.datetime.now())