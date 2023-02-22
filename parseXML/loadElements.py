import gc
import datetime
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
conn1.autocommit = True
cursor = conn1.cursor()

sql="""select r.rinok,(string_to_array((string_to_array(rc.value,':'))[1],'-'))[1] as rinok_values,r.entity,
r.parentrole,r.id,rc.value
from rulenodes r
join rulenodes_c rc on rc.rulenode_id=r.id and rc.parentrole=r.parentrole and r.nexttag='concept'
where (string_to_array((string_to_array(rc.value,':'))[1],'-'))[1] not in (select distinct rinok from elements)
"""
cursor.execute(sql)
result=cursor.fetchall()
result_list=[]
i=0
cbr='http://www.cbr.ru/xbrl/eps/cbr-coa'
ifrs='http://xbrl.ifrs.org/taxonomy/2021-03-24/ifrs-full'

for row in result:
    result_list.append({'rinok':row[0],'rinok_values':row[1],'entity':row[2],'parentrole':row[3],'id':row[4],'value':row[5]})
for xx in result_list:
    print(xx)
