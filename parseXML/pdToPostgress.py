import gc
import datetime
import parseDicNew,parseTab,parseMetaInf
import psycopg2
from sqlalchemy import create_engine

print('begin',datetime.datetime.now())
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

version='natasha'

#conn1.autocommit = True
cursor = conn1.cursor()

print('parseMetaInf', version)
ss=parseMetaInf.c_parseMeta(version)
df_list=ss.parseentry()
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

#['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']
for rinok in [['nfo','nfo'],['bki','bki'],['purcb','purcb'],['ins','ins']]:
    # try:
        print('parseTab', rinok)
        ss = parseTab.c_parseTab(version, rinok[0], rinok[1])
        df_list = ss.startParse()
        if len(df_list.get('df_tables').index)!=0:
            str_headers = ''
            for xx in df_list.keys():
                headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
                for hh in headers:
                    str_headers = str_headers + hh + '\n'
                str_headers = str_headers.strip()[:-1]
                df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
        del df_list
        gc.collect()

        print('parseDic', rinok)
        ss=parseDicNew.c_parseDic(version,rinok[0],rinok[1])
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

    # except:
    #     print(rinok,'not found')

ss=parseDicNew.c_parseDic(version,'udr\\dim','dim')
df_list=ss.startParse()
print('udr\\dim',df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

ss=parseDicNew.c_parseDic(version,'udr\\dom','mem')
df_list=ss.startParse()
print('udr\\dom',df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

ss=parseDicNew.c_parseDic(version,'bfo\\dict','dictionary')
df_list=ss.startParse()
print('bfo\\dict',df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

ss=parseDicNew.c_parseDic(version,'eps','cbr-coa')
df_list=ss.startParse()
print('eps',df_list.keys())
str_headers=''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

# ss=parseTab.c_parseTab(version,'bfo','bfo')
# df_list=ss.startParse()
# print(df_list.keys())
# str_headers=''
# for xx in df_list.keys():
#     headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
#     for hh in headers:
#         str_headers = str_headers + hh + '\n'
#     str_headers = str_headers.strip()[:-1]
#     df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
# del df_list
# gc.collect()

conn1.commit()
conn1.close()
print('end',datetime.datetime.now())