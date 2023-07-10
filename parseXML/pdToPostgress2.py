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

version='final_6_0_brk'
period='2023-03-31'

#conn1.autocommit = True
cursor = conn1.cursor()

print('parseMetaInf', version)
ss=parseMetaInf.c_parseMeta(version)
df_list= ss.parseentry() | ss.parsecatalog() | ss.parsetaxpackage()
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
del df_list
gc.collect()

# os.listdir(f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\rep\\{period}\\')

#['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']
#for rinok in [['operatory','oper']]:
for rinok in [['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']]:
    # try:
        print('parseTab', rinok)
        ss1 = parseTab.c_parseTab(version, rinok[0], rinok[1],period)
        df_list1 = ss1.startParse()
        if len(df_list1.get('df_tables').index)!=0:
            str_headers = ''
            for xx in df_list1.keys():
                headers = [xx.strip() + ' VARCHAR, ' for xx in df_list1.get(xx).keys().values]
                for hh in headers:
                    str_headers = str_headers + hh + '\n'
                str_headers = str_headers.strip()[:-1]
                df_list1.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
        del df_list1,ss1
        gc.collect()

        print('parseDic', rinok)
        ss2=parseDicNew.c_parseDic(version,rinok[0],rinok[1])
        df_list2=ss2.startParse()
        print(df_list2.keys())
        str_headers=''
        for xx in df_list2.keys():
            headers = [xx.strip() + ' VARCHAR, ' for xx in df_list2.get(xx).keys().values]
            for hh in headers:
                str_headers = str_headers + hh + '\n'
            str_headers = str_headers.strip()[:-1]
            df_list2.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
        del df_list2,ss2
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

ss=parseIFRS_FULL.c_parseIFRS_FULL(version)
df_list=ss.startParse()
print('ifrs-full',df_list.keys())
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
conn.close()
print('end',datetime.datetime.now())