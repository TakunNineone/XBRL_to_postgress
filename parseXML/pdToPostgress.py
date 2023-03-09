import gc
import datetime
import parseDicNew,parseTab
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

conn1.autocommit = True
cursor = conn1.cursor()
#['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']
for rinok in [['ins','ins']]:

    # print('parseDic',rinok)
    # ss=parseDicNew.c_parseDic('final_5_2',rinok[0],rinok[1])
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

    ss=parseTab.c_parseTab('final_5_2',rinok[0],rinok[1])
    print('parseTab',rinok)
    df_list=ss.startParse()
    str_headers=''
    for xx in df_list.keys():
        headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
        for hh in headers:
            str_headers = str_headers + hh + '\n'
        str_headers = str_headers.strip()[:-1]
        df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
    del df_list
    gc.collect()

# ss=parseDicNew.c_parseDic('final_5_2','udr\\dim','dim')
# df_list=ss.startParse()
# print('udr\\dim',df_list.keys())
# str_headers=''
# for xx in df_list.keys():
#     headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
#     for hh in headers:
#         str_headers = str_headers + hh + '\n'
#     str_headers = str_headers.strip()[:-1]
#     df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
# del df_list
# gc.collect()
#
# ss=parseDicNew.c_parseDic('final_5_2','udr\\dom','mem')
# df_list=ss.startParse()
# print('udr\\dom',df_list.keys())
# str_headers=''
# for xx in df_list.keys():
#     headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
#     for hh in headers:
#         str_headers = str_headers + hh + '\n'
#     str_headers = str_headers.strip()[:-1]
#     df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
# del df_list
# gc.collect()
#
# ss=parseDicNew.c_parseDic('final_5_2','bfo\\dict','dictionary')
# df_list=ss.startParse()
# print('bfo\\dict',df_list.keys())
# str_headers=''
# for xx in df_list.keys():
#     headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
#     for hh in headers:
#         str_headers = str_headers + hh + '\n'
#     str_headers = str_headers.strip()[:-1]
#     df_list.get(xx).to_sql(xx[3:],conn,if_exists= 'append',index=False)
# del df_list
# gc.collect()

# ss=parseTab.c_parseTab('final_5_2','bfo','bfo')
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

# conn1.commit()
# conn1.close()
print('end',datetime.datetime.now())