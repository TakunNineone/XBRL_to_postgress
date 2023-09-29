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
sql_delete = """
delete from arcs;
delete from aspectnodes;
delete from catalog;
delete from elements;
--drop elements_labels;
delete from entrypoints;
delete from labels;
delete from linkbaserefs;
delete from locators;
delete from messages;
delete from preconditions;
--drop preferred_labels;
delete from rend_edimensions;
delete from rend_edmembers;
delete from rolerefs;
delete from roletypes;
delete from rulenodes;
delete from rulenodes_c;
delete from rulenodes_e;
delete from rulenodes_p;
delete from rulesets;
delete from tableparts;
delete from tables;
delete from tableschemas;
delete from taxpackage;
delete from va_aspectcovers;
delete from va_assertions;
delete from va_assertionsets;
delete from va_concepts;
delete from va_edimensions;
delete from va_edmembers;
delete from va_factvars;
delete from va_generals;
delete from va_tdimensions;
"""
sql_create_functions = """
CREATE OR REPLACE FUNCTION public.array_unique(
	arr anyarray)
    RETURNS text[]
    LANGUAGE 'sql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
select array( select x from (select distinct unnest($1) as x) z where x is not null  order by 1)
$BODY$;

CREATE OR REPLACE FUNCTION public.check_similarity(
	str1 text,
	arr_str text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    found boolean := false;
BEGIN
    FOR i IN 1..array_length(arr_str, 1) LOOP
        IF  arr_str[i] similar to str1||'\D%' or str1=arr_str[i] THEN
            found := true;
            EXIT;
        END IF;
    END LOOP;

    IF found THEN
        RETURN 1;
    ELSE
        RETURN 0;
    END IF;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.compare_arrays(
	arr1 text[],
	arr2 text[])
    RETURNS integer
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    compare_elements text[];
    subarrays text[];
	subarr text;
	check_element text;
	check_ int :=1;
BEGIN
    -- Если arr2 равен NULL, возвращаем 0
    IF arr2 IS NULL THEN
        RETURN 0;
    END IF;

    -- Разбиваем элементы arr2 на подмассивы
    FOREACH subarr IN ARRAY arr2
    LOOP
        subarrays := subarrays || string_to_array(subarr, ';');
    END LOOP;

    -- Убираем дубликаты и пустые строки
    subarrays := array_remove(subarrays, NULL);
    subarrays := array_remove(subarrays, '');

    -- Находим элементы для сравнения (которые есть в arr1 и в subarrays)
    SELECT array_agg(DISTINCT elem)
    INTO compare_elements
    FROM unnest(arr1) AS elem
    WHERE elem = ANY(subarrays);
	if compare_elements is null then
	return 0;
	end if;

  	FOREACH subarr IN ARRAY arr2
		loop
			FOREACH check_element in array compare_elements
				loop
					if check_element = ANY (string_to_array(subarr, ';')) then
					else
					check_=0;
					end if;
				end loop;
			if check_=1 then
				return 1;
			else
				check_=1;
			end if;
		end loop;
		return 0;
END;
$BODY$;

CREATE OR REPLACE FUNCTION public.delete_space_and_tab(
	text)
    RETURNS text
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
declare
    text_line text;
 m   varchar;
 res text array;
 ret_txt text;
BEGIN
   text_line=replace(replace(replace(replace($1,
        ' ', ' '),
        E'\n', ' '),
        E'\t', ' '),
        E'\r', ' ');
   FOREACH m IN ARRAY string_to_array(text_line,' ')
   loop
 if trim(m)!='' then
  res=array_append(res,m);
 end if;
 end loop;
ret_txt:=array_to_string(res,' ');
return ret_txt;
END;
$BODY$;

	
		
	CREATE OR REPLACE FUNCTION public.generate_combinations(
	input_array text[])
    RETURNS text[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE
    result text[] := '{}';
    temp text[];
    i integer;
    j integer;
BEGIN
    -- Перебираем все элементы входного массива
    FOR i IN 1..array_length(input_array, 1) LOOP
        -- Разбиваем текущий элемент на подэлементы
        temp := string_to_array(input_array[i], '|');
        -- Если результирующий массив пустой, добавляем первый подэлемент
        IF result = '{}' THEN
            result := temp;
        ELSE
            -- Создаем временный массив для хранения новых комбинаций
            -- на основе текущего результата и текущего подэлемента
            temp := array(
                SELECT array_agg(elem1 || '|' || elem2)
                FROM (select unnest(result) AS elem1 limit 100000) AS elem1, (select unnest(temp) AS elem2 limit 100000) AS elem2
            );
            result := temp;
        END IF;
    END LOOP;
    
    RETURN result;
END;
$BODY$;


CREATE OR REPLACE FUNCTION public.remove_elements_from_array(
	arr1 text[],
	arr2 text[])
    RETURNS text[]
    LANGUAGE 'plpgsql'
    COST 100
    VOLATILE PARALLEL UNSAFE
AS $BODY$
DECLARE    result text[] := arr1;
BEGIN
    if arr1 is not null and arr2 is not null then
		FOR i IN 1..array_length(arr2, 1) LOOP        
		result := array_remove(result, arr2[i]);
		END LOOP;    
		RETURN result;
	else 
	return(arr1);
	end if;
END;
$BODY$;
"""
sql_indexes = """
create index arcs_v on arcs (version);
create index elements_v  on elements (version);
create index labels_v  on labels (version);
create index linkbaserefs_v  on linkbaserefs (version);
create index locators_v  on locators (version);
create index rolerefs_v  on rolerefs (version);
create index roletypes_v  on roletypes (version);
create index rulenodes_v  on rulenodes (version);
create index rulenodes_c_v  on rulenodes_c (version);
create index rulenodes_e_v  on rulenodes_e (version);
create index rulenodes_p_v  on rulenodes_p (version);
create index tables_v  on tables (version);
create index tableschemas_v  on tableschemas (version);
create index va_assertions_v  on va_assertions (version);
create index va_concepts_v  on va_concepts (version);
create index va_edimensions_v  on va_edimensions (version);
create index va_edmembers_v  on va_edmembers (version);
create index va_factvars_v  on va_factvars (version);
create index va_tdimensions_v  on va_tdimensions (version);
create index va_generals_v  on va_generals (version);
create index tableparts_v  on tableparts (version);
create index va_aspectcovers_v  on va_aspectcovers (version);
create index va_assertionsets_v  on va_assertionsets (version);
create index aspectnodes_v  on aspectnodes (version);
create index entrypoints_v on entrypoints (version);
create index rend_edimensions_v  on rend_edimensions (version);
create index rend_edmembers_v  on rend_edmembers (version);
create index rulesets_v  on rulesets (version);
create index catalog_v  on catalog (version);
create index taxpackage_v  on taxpackage (version);

create index arcs_entity on arcs (entity);
create index elements_entity  on elements (entity);
create index labels_entity  on labels (entity);
create index linkbaserefs_entity  on linkbaserefs (entity);
create index locators_entity  on locators (entity);
create index rolerefs_entity  on rolerefs (entity);
create index roletypes_entity  on roletypes (entity);
create index rulenodes_entity  on rulenodes (entity);
create index rulenodes_c_entity  on rulenodes_c (entity);
create index rulenodes_e_entity  on rulenodes_e (entity);
create index rulenodes_p_entity  on rulenodes_p (entity);
create index tables_entity  on tables (entity);
create index tableschemas_entity  on tableschemas (entity);
create index va_assertions_entity  on va_assertions (entity);
create index va_concepts_entity  on va_concepts (entity);
create index va_edimensions_entity  on va_edimensions (entity);
create index va_edmembers_entity  on va_edmembers (entity);
create index va_factvars_entity  on va_factvars (entity);
create index va_tdimensions_entity  on va_tdimensions (entity);
create index va_generals_entity  on va_generals (entity);
create index tableparts_entity  on tableparts (entity);
create index va_aspectcovers_entity  on va_aspectcovers (entity);
create index va_assertionsets_entity  on va_assertionsets (entity);
create index aspectnodes_entity  on aspectnodes (entity);
create index rend_edimensions_entity  on rend_edimensions (entity);
create index rend_edmembers_entity  on rend_edmembers (entity);
create index rulesets_entity  on rulesets (entity);

create index arcs_parentrole on arcs (parentrole);
create index labels_parentrole  on labels (parentrole);
create index locators_parentrole  on locators (parentrole);
create index rulenodes_parentrole  on rulenodes (parentrole);
create index rulenodes_c_parentrole  on rulenodes_c (parentrole);
create index rulenodes_e_parentrole  on rulenodes_e (parentrole);
create index rulenodes_p_parentrole  on rulenodes_p (parentrole);
create index tableschemas_parentrole  on tableschemas (parentrole);
create index va_assertions_parentrole  on va_assertions (parentrole);
create index va_concepts_parentrole  on va_concepts (parentrole);
create index va_edimensions_parentrole  on va_edimensions (parentrole);
create index va_edmembers_parentrole  on va_edmembers (parentrole);
create index va_factvars_parentrole  on va_factvars (parentrole);
create index va_tdimensions_parentrole  on va_tdimensions (parentrole);
create index va_generals_parentrole  on va_generals (parentrole);
create index va_aspectcovers_parentrole  on va_aspectcovers (parentrole);
create index va_assertionsets_parentrole  on va_assertionsets (parentrole);
create index aspectnodes_parentrole  on aspectnodes (parentrole);
create index rend_edimensions_parentrole  on rend_edimensions (parentrole);
create index rend_edmembers_parentrole  on rend_edmembers (parentrole);
create index rulesets_parentrole  on rulesets (parentrole);

create index arcs_rinok on arcs (rinok);
create index elements_rinok  on elements (rinok);
create index labels_rinok  on labels (rinok);
create index linkbaserefs_rinok  on linkbaserefs (rinok);
create index locators_rinok  on locators (rinok);
create index rolerefs_rinok  on rolerefs (rinok);
create index roletypes_rinok  on roletypes (rinok);
create index rulenodes_rinok  on rulenodes (rinok);
create index rulenodes_c_rinok  on rulenodes_c (rinok);
create index rulenodes_e_rinok  on rulenodes_e (rinok);
create index rulenodes_p_rinok  on rulenodes_p (rinok);
create index tables_rinok  on tables (rinok);
create index tableschemas_rinok  on tableschemas (rinok);
create index va_assertions_rinok  on va_assertions (rinok);
create index va_concepts_rinok  on va_concepts (rinok);
create index va_edimensions_rinok  on va_edimensions (rinok);
create index va_edmembers_rinok  on va_edmembers (rinok);
create index va_factvars_rinok  on va_factvars (rinok);
create index va_tdimensions_rinok  on va_tdimensions (rinok);
create index va_generals_rinok  on va_generals (rinok);
create index tableparts_rinok  on tableparts (rinok);
create index va_aspectcovers_rinok  on va_aspectcovers (rinok);
create index va_assertionsets_rinok  on va_assertionsets (rinok);
create index aspectnodes_rinok  on aspectnodes (rinok);
create index rend_edimensions_rinok  on rend_edimensions (rinok);
create index rend_edmembers_rinok  on rend_edmembers (rinok);
create index rulesets_rinok  on rulesets (rinok);

create index locators_href_id on locators (href_id);
create index locators_label on locators (label);
create index arcs_arcto on arcs (arcto);
create index arcs_arcfrom on arcs (arcfrom);

create index messages_v on messages (version);
create index messages_entity on messages (entity);
create index messages_parentrole on messages (parentrole);
create index messages_rinok on messages (rinok);
"""
sql_create_elements_labels = """
create table elements_labels as 
select e.version,e.rinok,e.entity,e.name,e.id,e.qname,e.type,e.substitutiongroup,la.lang,la.label,la.role,e.abstract,la.text
from 
elements e 
left join locators le ON le.href_id = e.id AND le.rinok = e.rinok AND le.locfrom = 'label' and le.version=e.version
left join arcs ae ON ae.rinok = le.rinok AND ae.entity = le.entity AND ae.arcfrom = le.label 
AND ae.arctype = 'label' and ae.version=le.version
left join labels la ON la.rinok = ae.rinok AND la.entity = ae.entity AND la.label = ae.arcto and la.version=ae.version
"""
sql_create_preferred_labels = """
create table preferred_labels as
select a.version,a.rinok,a.entity,a.parentrole,l.href_id,text
from arcs a
join locators l on l.label=a.arcto and l.version=a.version and l.rinok=a.rinok and l.entity=a.entity and a.parentrole=l.parentrole	
join elements_labels el on el.version=l.version and el.id=l.href_id
where arctype='presentation' and preferredlabel is not null
and el.role=a.preferredlabel;
"""
# cursor.execute(sql_delete)
# conn1.commit()

print('parseMetaInf', version)
ss = parseMetaInf.c_parseMeta(version)
df_list = ss.parseentry() | ss.parsecatalog() | ss.parsetaxpackage()
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

# ['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['npf','npf'],['srki','srki'],['sro','sro']
# ['ins','ins'],['uk','uk'],['purcb','purcb'],['brk','brk'],['kra','kra'],['nfo','nfo'],['npf','npf'],['bki','bki'],['srki','srki']
# for rinok in [['operatory','oper']]:
for rinok in [['npf','npf'],['uk','uk'],['purcb','purcb'],['operatory','oper'],['bki','bki'],['brk','brk'],['ins','ins'],['kra','kra'],['nfo','nfo'],['srki','srki'],['sro','sro']]:
    # try:
    print('parseTab', rinok)
    ss1 = parseTab.c_parseTab(version, rinok[0], rinok[1], period)
    df_list1 = ss1.startParse()
    if len(df_list1.get('df_tables').index) != 0:
        str_headers = ''
        for xx in df_list1.keys():
            headers = [xx.strip() + ' VARCHAR, ' for xx in df_list1.get(xx).keys().values]
            for hh in headers:
                str_headers = str_headers + hh + '\n'
            str_headers = str_headers.strip()[:-1]
            df_list1.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
    del df_list1, ss1
    gc.collect()

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

# except:
#     print(rinok,'not found')

ss = parseDicNew.c_parseDic(version, 'udr\\dim', 'dim')
df_list = ss.startParse()
print('udr\\dim', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseDicNew.c_parseDic(version, 'udr\\dom', 'mem')
df_list = ss.startParse()
print('udr\\dom', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseDicNew.c_parseDic(version, 'bfo\\dict', 'dictionary')
df_list = ss.startParse()
print('bfo\\dict', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseIFRS_FULL.c_parseIFRS_FULL(version)
df_list = ss.startParse()
print('ifrs-full', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()

ss = parseDicNew.c_parseDic(version, 'eps', 'cbr-coa')
df_list = ss.startParse()
print('eps', df_list.keys())
str_headers = ''
for xx in df_list.keys():
    headers = [xx.strip() + ' VARCHAR, ' for xx in df_list.get(xx).keys().values]
    for hh in headers:
        str_headers = str_headers + hh + '\n'
    str_headers = str_headers.strip()[:-1]
    df_list.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
del df_list
gc.collect()


cursor.execute(sql_indexes)
cursor.execute(sql_create_functions)
cursor.execute(sql_create_elements_labels)
cursor.execute(sql_create_preferred_labels)

# print('parseTab', 'bfo')
# ss1 = parseTab.c_parseTab(version, 'bfo', 'bfo', period)
# df_list1 = ss1.startParse()
# print(df_list1.keys())
# if len(df_list1.get('df_tables').index) != 0:
#     str_headers = ''
#     for xx in df_list1.keys():
#         print(xx)
#         headers = [xx.strip() + ' VARCHAR, ' for xx in df_list1.get(xx).keys().values]
#         for hh in headers:
#             str_headers = str_headers + hh + '\n'
#         str_headers = str_headers.strip()[:-1]
#         df_list1.get(xx).to_sql(xx[3:], conn, if_exists='append', index=False)
# del df_list1, ss1
# gc.collect()

conn1.commit()
conn1.close()
conn.close()
print('end', datetime.datetime.now())