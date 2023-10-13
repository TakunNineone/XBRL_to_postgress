import gc
import datetime
import parseDicNew, parseTab, parseMetaInf, parseIFRS_FULL
import psycopg2
from sqlalchemy import create_engine

print('begin', datetime.datetime.now())
conn_string = 'postgresql+psycopg2://postgres:124kosm21@127.0.0.1/final_5_2'

db = create_engine(conn_string)
conn = db.connect()
conn1 = psycopg2.connect(user="postgres",
                         password="124kosm21",
                         host="127.0.0.1",
                         port="5432",
                         database="final_5_2")
print(conn)
print(conn1)

version = 'final_5_2'
period = '2023-03-31'

cursor = conn1.cursor()


print('parseTab', 'bfo')
ss1 = parseTab.c_parseTab(version, 'bfo', 'bfo', period)
df_list1 = ss1.startParse()
print(df_list1.keys())
if len(df_list1.get('df_tables').index) != 0:
    str_headers = ''
    for xx in df_list1.keys():
        print(xx)
        headers = [xx.strip() + ' VARCHAR, ' for xx in df_list1.get(xx).keys().values]
        for hh in headers:
            str_headers = str_headers + hh + '\n'
        str_headers = str_headers.strip()[:-1]
        df_list1.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list1, ss1
gc.collect()


conn1.commit()
conn1.close()
conn.close()
print('end', datetime.datetime.now())