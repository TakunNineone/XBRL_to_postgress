import gc
import re

import  pandas as pd
import os
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

class c_parseToDf():
    def __init__(self,taxonomy,rinok):
        self.rinok=rinok
        self.version=taxonomy
        self.df_rulenodes_Dic = []
        self.df_aspectnodes_Dic = []
        self.df_rulenodes_e_Dic=[]
        self.df_rulenodes_c_Dic = []
        self.df_rulenodes_p_Dic = []
        self.df_rulesets_Dic=[]
        self.df_rend_edmembers_Dic = []
        self.df_rend_edimensions_Dic = []
        self.df_elements_Dic=[]
        self.df_element_attrs_Dic=[]
        self.df_roletypes_Dic=[]
        self.df_locators_Dic=[]
        self.df_arcs_Dic=[]
        self.df_labels_Dic=[]
        self.df_rolerefs_Dic=[]
        self.df_tableschemas_Dic=[]
        self.df_linkbaserefs_Dic=[]
        self.df_tableparts_Dic=[]
        self.df_va_edmembers_Dic = []
        self.df_va_edimensions_Dic = []
        self.df_va_tdimensions_Dic = []
        self.df_va_concepts_Dic = []
        self.df_va_factvars_Dic = []
        self.df_va_assertions_Dic = []
        self.df_va_generals_Dic=[]
        self.df_va_aspectcovers_Dic=[]
        self.df_va_assertionset_Dic=[]
        self.df_preconditions_Dic=[]
        self.df_messages_Dic=[]
        self.df_tables = pd.DataFrame(columns=['version', 'rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace'])

    def parse_precond(self,soup,path):
        temp_list = []
        columns = ['version', 'rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'test']
        soup = soup.find_all_next(re.compile('.*precondition$'))
        for xx in soup:
            parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                    parentrole,
                                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                    xx['id'] if 'id' in xx.attrs.keys() else None,
                                    xx['test'] if 'test' in xx.attrs.keys() else None
                                    ])
        df_preconditions=pd.DataFrame(data=temp_list,columns=columns)
        self.df_preconditions_Dic.append(df_preconditions)
        del df_preconditions,temp_list

    def parse_messages(self,soup,path):
        temp_list = []
        columns = ['version', 'rinok', 'entity', 'parentrole', 'type', 'label','role', 'title','lang', 'id', 'text']
        soup = soup.find_all_next(re.compile('.*message$'))
        for xx in soup:
            parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
            temp_list.append([self.version, self.rinok, os.path.basename(path),
                              parentrole,
                              xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                              xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                              xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else None,
                              xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                              xx['xml:lang'] if 'xml:lang' in xx.attrs.keys() else None,
                              xx['id'] if 'id' in xx.attrs.keys() else None,
                              xx.text
                              ])
        df_messages = pd.DataFrame(data=temp_list, columns=columns)
        self.df_messages_Dic.append(df_messages)
        del df_messages, temp_list

    def parse_assertions(self,soup,path,tag):
        # print(f'parse_assertions - {path}')
        temp_list = []
        columns=['version','rinok', 'entity', 'parentrole','assert_type', 'type', 'label', 'title', 'id', 'test','variables', 'aspectmodel','implicitfiltering']
        # soup=soup.find_all_next(re.compile('.*valueassertion$'))
        for xx in soup:
            parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                    parentrole,
                                    tag,
                                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                    xx['id'] if 'id' in xx.attrs.keys() else None,
                                    xx['test'] if 'test' in xx.attrs.keys() else None,
                                    '',
                                    xx['aspectmodel'] if 'aspectmodel' in xx.attrs.keys() else None,
                                    xx['implicitfiltering'] if 'implicitfiltering' in xx.attrs.keys() else None
                                    ])
        df_va_assertions=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_assertions_Dic.append(df_va_assertions)
        del df_va_assertions

    def parse_assertionset(self,soup,path):
        # print(f'parse_concepts - {path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id']
        soup=soup.find_all_next(re.compile('.*assertionset$'))
        for xx in soup:
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None
                                        ])
        df_va_assertionset=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_assertionset_Dic.append(df_va_assertionset)
        del df_va_assertionset,temp_list

    def parse_aspectcovers(self,soup,path):
        # print(f'parse_aspectcovers - {path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole','type', 'label', 'title', 'id','aspects']
        for xx in soup:
            aspects = [yy.text for yy in xx.find_all(re.compile('.*aspect$'))]
            aspects_str=''
            for yy in aspects:
                aspects_str=aspects_str+yy+' '
            aspects_str=aspects_str.strip()
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                      xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                      xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                      xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                      xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                      xx['id'] if 'id' in xx.attrs.keys() else None,
                                      aspects_str
                                      ])
        df_va_aspectcovers=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_aspectcovers_Dic.append(df_va_aspectcovers)
        del df_va_aspectcovers,temp_list

    def parse_generals(self,soup,path):
        # print(f'parse_generals - {path}')
        temp_list = []
        columns=['version','rinok', 'entity', 'parentrole','label','title','id','test']
        for xx in soup:
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['test'] if 'test' in xx.attrs.keys() else None,
                                        ])
        df_va_generals=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_generals_Dic.append(df_va_generals)
        del df_va_generals,temp_list

    def parse_factvars(self,soup,path):
        # print(f'parse_factvars - {path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'bindassequence','fallbackvalue']
        soup=soup.find_all_next(re.compile('.*factvariable$'))
        for xx in soup:
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['bindassequence'] if 'bindassequence' in xx.attrs.keys() else None,
                                        xx['fallbackvalue'] if 'fallbackvalue' in xx.attrs.keys() else None
                                        ])
        df_va_factvars=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_factvars_Dic.append(df_va_factvars)
        del df_va_factvars,temp_list

    def parse_concepts(self,soup,path):
        # print(f'parse_concepts - {path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'value']
        soup=soup.find_all_next(re.compile('.*conceptname$'))
        for xx in soup:
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx.text.strip()
                                        ])
        df_va_concepts=pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_concepts_Dic.append(df_va_concepts)
        del df_va_concepts,temp_list

    def parse_tdimensions(self,soup,path):
        # print(f'parse_tdimensions - {path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'value']
        soup=soup.find_all_next(re.compile('.*typeddimension$'))
        for xx in soup:
            temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx.text.strip()
                                        ])
        df_va_tdimensions = pd.DataFrame(data=temp_list,columns=columns)
        self.df_va_tdimensions_Dic.append(df_va_tdimensions)
        del df_va_tdimensions,temp_list

    def generator_(self,iterable):
        iterator = iter(iterable)
        for yy in iterator:
            member=yy.find(re.compile('.*qname$')).text if yy.find(re.compile('.*qname$')) else None
            linkrole=yy.find(re.compile('.*linkrole$')).text if yy.find(re.compile('.*linkrole$')) else None
            arcrole=yy.find(re.compile('.*arcrole$')).text if yy.find(re.compile('.*arcrole$')) else None
            axis=yy.find(re.compile('.*axis$')).text if  yy.find(re.compile('.*axis$')) else None
            yield (member,linkrole,arcrole,axis)
        # yield (member,linkrole,arcrole,axis)


    def parse_edimensions(self,soup,path):
        # print(f'parse_edimensions - {path}')
        temp_list1=[]
        temp_list2=[]
        columns1=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'dimension']
        columns2=['version','rinok', 'entity', 'parentrole', 'dimension_id', 'member','linkrole','arcrole','axis']
        soup=soup.find_all_next(re.compile('.*explicitdimension$'))
        for parentrole,type,label,title,id,dimension,members in self.generator_2(soup):
            temp_list1.append([self.version,self.rinok, os.path.basename(path),
                               parentrole, type, label, title, id, dimension
                                        ])
            if members!=[]:
                for member, linkrole, arcrole, axis in self.generator_(members):
                    temp_list2.append([self.version, self.rinok, os.path.basename(path),
                                       parentrole,
                                       id, member, linkrole, arcrole, axis])
        df_va_edimensions=pd.DataFrame(data=temp_list1,columns=columns1)
        df_va_edmembers=pd.DataFrame(data=temp_list2,columns=columns2)
        self.df_va_edmembers_Dic.append(df_va_edmembers)
        self.df_va_edimensions_Dic.append(df_va_edimensions)
        del df_va_edmembers,df_va_edimensions,temp_list1,temp_list2


    def generator_2(self,iterable):
        iterator = iter(iterable)
        for xx in iterator:
            parentrole=xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None
            type=xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None
            label=xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None
            title=xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None
            id=xx['id'] if 'id' in xx.attrs.keys() else None
            dimension=xx.findChild().text.strip()
            members=xx.find_all(re.compile('.*member$'))
            yield (parentrole,type,label,title,id,dimension,members)
        # yield (parentrole, type, label, title, id, dimension, members)


    def parse_edimensions_rend(self,soup,path):
        # print(f'parse_edimensions - {path}')
        temp_list1=[]
        temp_list2=[]
        columns1=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'dimension']
        columns2=['version','rinok', 'entity', 'parentrole', 'dimension_id', 'member','linkrole','arcrole','axis']
        soup=soup.find_all_next(re.compile('df:explicitdimension$'))
        for parentrole, type, label, title, id, dimension, members in self.generator_2(soup):
            temp_list1.append([self.version, self.rinok, os.path.basename(path),
                               parentrole, type, label, title, id, dimension
                               ])
            if members!=[]:
                for member,linkrole,arcrole,axis in self.generator_(members):
                   temp_list2.append([self.version,self.rinok, os.path.basename(path),
                                        parentrole,
                                        id,member, linkrole, arcrole, axis])
        df_rend_edimensions=pd.DataFrame(data=temp_list1,columns=columns1)
        df_rend_edmembers=pd.DataFrame(data=temp_list2,columns=columns2)
        self.df_rend_edmembers_Dic.append(df_rend_edmembers)
        self.df_rend_edimensions_Dic.append(df_rend_edimensions)
        del df_rend_edmembers,df_rend_edimensions,temp_list1,temp_list2

    # def parseVA(self,path):
    #     df_va_covers = pd.DataFrame(columns=['rinok', 'entity', 'parentrole','type', 'label', 'title', 'id','values'])

    def parseAspectnodes(self,path):
        soup=self.parsetag(path,'linkbase').find_all_next('table:aspectnode')
        temp_list = []
        columns = ['version', 'rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id','includeunreportedvalue','dimension','period']
        if soup:
            for xx in soup:
                parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None
                if xx.find(re.compile('.*dimensionaspect$')):
                    includeunreportedvalue,dimension=xx.find(re.compile('.*dimensionaspect$'))['includeunreportedvalue'] if 'includeunreportedvalue' in xx.find(re.compile('.*dimensionaspect$')).attrs.keys() else None,xx.find(re.compile('.*dimensionaspect$')).text
                else:
                    includeunreportedvalue, dimension = None, None
                if xx.find(re.compile('.*periodaspect$')):
                    period = xx.find(re.compile('.*periodaspect$')).text
                else:
                    period = None
                temp_list.append([self.version,self.rinok, os.path.basename(path),
                                        parentrole,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        includeunreportedvalue,dimension,period
                                        ])
        df_aspectnodes = pd.DataFrame(data=temp_list, columns=columns)
        self.appendDfs_Dic(self.df_aspectnodes_Dic, df_aspectnodes)
        del df_aspectnodes, temp_list

    def parseRulenodes(self,path):
        # try:
        soup=self.parsetag(path,'linkbase').find_all_next(re.compile('.*rulenode$'))
        temp_list1 = []
        temp_list2 = []
        temp_list3 = []
        temp_list4 = []
        columns1=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'abstract', 'merge','tagselector']
        columns2=['version','rinok', 'entity', 'parentrole', 'rulenode_id', 'dimension', 'member']
        columns3=['version','rinok', 'entity', 'parentrole', 'rulenode_id','value']
        columns4=['version','rinok', 'entity', 'parentrole', 'rulenode_id','period_type','start','end']
        if soup:
            for xx in soup:
                parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None
                nexttag_e = xx.find_all(re.compile('formula:explicitdimension$'))
                nexttag_p = xx.find_all(re.compile('formula:period$'))
                nexttag_c = xx.find_all(re.compile('formula:concept$'))
                temp_list1.append([self.version,self.rinok, os.path.basename(path),
                                        parentrole,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['abstract'] if 'abstract' in xx.attrs.keys() else None,
                                        xx['merge'] if 'merge' in xx.attrs.keys() else None,
                                        xx['tagselector'] if 'tagselector' in xx.attrs.keys() else None
                                        ])
                if nexttag_e:
                    for ee in nexttag_e:
                        temp_list2.append([self.version,self.rinok, os.path.basename(path),
                                                         parentrole,
                                                         xx['id'] if 'id' in xx.attrs.keys() else None,
                                                         ee['dimension'] if 'dimension' in ee.attrs.keys() else None,
                                                         ee.text.strip()
                                                         ])
                if nexttag_c:
                    for cc in nexttag_c:
                        temp_list3.append([self.version,self.rinok, os.path.basename(path),
                                                       parentrole,
                                                       xx['id'] if 'id' in xx.attrs.keys() else None,
                                                       cc.text.strip()
                                                       ])
                if nexttag_p:
                    for pp in nexttag_p:
                        temp_list4.append([self.version,self.rinok, os.path.basename(path),
                                                       parentrole,
                                                       xx['id'] if 'id' in xx.attrs.keys() else None,
                                                       pp.find().name.replace('formula:',''),
                                                       pp.find()['start'] if 'start' in pp.find().attrs.keys() else pp.find()['value'] if 'value' in pp.find().attrs.keys() else None,
                                                       pp.find()['end'] if 'end' in pp.find().attrs.keys() else None
                                                       ])
        df_rulenodes=pd.DataFrame(data=temp_list1,columns=columns1)
        df_rulenodes_e=pd.DataFrame(data=temp_list2,columns=columns2)
        df_rulenodes_c=pd.DataFrame(data=temp_list3, columns=columns3)
        df_rulenodes_p=pd.DataFrame(data=temp_list4, columns=columns4)

        self.appendDfs_Dic(self.df_rulenodes_Dic,df_rulenodes)
        self.appendDfs_Dic(self.df_rulenodes_c_Dic, df_rulenodes_c)
        self.appendDfs_Dic(self.df_rulenodes_p_Dic, df_rulenodes_p)
        self.appendDfs_Dic(self.df_rulenodes_e_Dic, df_rulenodes_e)
        del df_rulenodes_e,df_rulenodes_p,df_rulenodes_c,df_rulenodes,temp_list1,temp_list2,temp_list3,temp_list4

    def generator_rulesets(self, iterable):
        iterator = iter(iterable)
        for xx in iterator:
            parentrole = xx.parent.parent['xlink:role'] if 'xlink:role' in xx.parent.parent.attrs.keys() else None
            rulenode_id = xx.parent['id'] if 'id' in xx.parent.attrs.keys() else None
            tag = xx['tag'] if 'tag' in xx.attrs.keys() else None
            if xx.find(re.compile('.*period$')):
                period_instant = xx.find(re.compile('.*period$')).find(re.compile('.*instant$'))['value'] if xx.find(re.compile('.*period$')).find(re.compile('.*instant$')) else None
                period_start = xx.find(re.compile('.*period$')).find(re.compile('.*duration$'))['start'] if xx.find(re.compile('.*period$')).find(re.compile('.*duration$')) else None
                period_end = xx.find(re.compile('.*period$')).find(re.compile('.*duration$'))['end'] if xx.find(re.compile('.*period$')).find(re.compile('.*duration$')) else None
            else:
                period_instant,period_start,period_end = None,None,None
            concept = xx.find(re.compile('.*concept$')).find(re.compile('.*qname$')).text if xx.find(re.compile('.*concept$')) else None
            dimension = xx.find(re.compile('.*explicitdimension$'))['dimension'] if xx.find(re.compile('.*explicitdimension$')) else None
            member = xx.find(re.compile('.*explicitdimension$')).find(re.compile('.*member$')).find(re.compile('.*qname$')).text if xx.find(re.compile('.*explicitdimension$')) else None
            yield (parentrole, rulenode_id, tag, period_instant,period_start,period_end,concept,dimension,member)
        # yield (member,linkrole,arcrole,axis)

    def parseRulesets(self,path):
        soup=self.parsetag(path,'linkbase').find_all_next(re.compile('.*ruleset$'))
        columns=['version','rinok', 'entity','parentrole', 'rulenode_id','tag', 'per_instant','per_start','per_end','concept','dimension','member']
        temp_list=[]
        for parentrole, rulenode_id, tag, period_instant,period_start,period_end,concept,dimension,member in self.generator_rulesets(soup):
            temp_list.append([self.version,self.rinok,os.path.basename(path),parentrole,rulenode_id,tag,period_instant,period_start,period_end,concept,dimension,member])
        df_rulesets=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_rulesets_Dic,df_rulesets)
        del temp_list,df_rulesets

    def parseElements(self,dict_with_lbrfs, full_file_path):
        #print(f'Elements - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'targetnamespace', 'name', 'id','qname', 'type',
                                                 'typeddomainref', 'substitutiongroup', 'periodtype', 'abstract',
                                                 'nillable', 'creationdate', 'fromdate', 'enumdomain', 'enum2domain',
                                                 'enumlinkrole', 'enum2linkrole','pattern','minlength']
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                qname_rep=os.path.basename(full_file_path).replace('.xsd','')
                if self.rinok == 'bfo':
                    qname_rep = 'ifrs-ru'
                if self.rinok == 'ifrs-full':
                    qname_rep = 'ifrs-full'
                    rinok='bfo'
                    entity='dictionary.xsd'
                if self.rinok=='eps':
                    qname_rep='cbr-coa-dic'
                    rinok=self.rinok
                    entity=os.path.basename(full_file_path)
                else:
                    rinok=self.rinok
                    entity=os.path.basename(full_file_path)

                restriction=xx.find('xsd:restriction')
                pattern = None
                minlength = None
                if restriction:
                    #print(xx['name'])
                    attrs=restriction.findChildren()
                    for aa in attrs:
                        if re.search('.*pattern$',aa.name):
                            pattern = aa['value']
                        elif re.search('.*minlength$',aa.name):
                            minlength = aa['value']
                temp_list.append([
                    self.version,rinok,entity,
                    xx.parent['targetnamespace'] if 'targetnamespace' in xx.parent.attrs.keys() else None,
                    xx['name'] if 'name' in xx.attrs else None,
                    xx['id'] if 'id' in xx.attrs else None,
                    xx['id'].replace(qname_rep+'_',qname_rep+':') if 'id' in xx.attrs else None,
                    xx['type'] if 'type' in xx.attrs else None,
                    xx['xbrldt:typeddomainref'] if 'xbrldt:typeddomainref' in xx.attrs else None,
                    xx['substitutiongroup'] if 'substitutiongroup' in xx.attrs else None,
                    xx['xbrli:periodtype'] if 'xbrli:periodtype' in xx.attrs else None,
                    xx['abstract'] if 'abstract' in xx.attrs else None,
                    xx['nillable'] if 'nillable' in xx.attrs else None,
                    xx['model:creationdate'] if 'model:creationdate' in xx.attrs else None,
                    xx['model:fromdate'] if 'model:fromdate' in xx.attrs else None,
                    xx['enum:domain'] if 'enum:domain' in xx.attrs else None,
                    xx['enum2:domain'] if 'enum2:domain' in xx.attrs else None,
                    xx['enum:linkrole'] if 'enum:linkrole' in xx.attrs else None,
                    xx['enum2:linkrole'] if 'enum2:linkrole' in xx.attrs else None,
                    pattern,
                    minlength
                ])
        df_elements=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_elements_Dic, df_elements)
        del df_elements,temp_list

    def parseTableParts(self, soup, full_file_path):
        # print(f'Linkbaserefs - {full_file_path}')
        temp_list = []
        columns = ['version', 'rinok', 'entity', 'uri_table', 'uri_razdel', 'id']
        dict_with_lbrfs = soup.find_all(re.compile('.*roletype$'))
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                temp_list.append([self.version, self.rinok, os.path.basename(full_file_path),
                                  xx.parent.parent.parent['targetnamespace'] if 'targetnamespace' in xx.parent.parent.parent.attrs.keys() else None,
                                  xx['roleuri'] if 'roleuri' in xx.attrs.keys() else None,
                                  xx['id'] if 'id' in xx.attrs.keys() else None
                                  ])
        df_tableparts = pd.DataFrame(data=temp_list, columns=columns)
        self.appendDfs_Dic(self.df_tableparts_Dic, df_tableparts)
        del df_tableparts, temp_list

    def parseLinkbaserefs(self, soup, full_file_path):
        #print(f'Linkbaserefs - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'targetnamespace', 'type', 'href']
        dict_with_lbrfs=soup.find_all(re.compile('.*linkbaseref$'))
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                temp_list.append([self.version,self.rinok, os.path.basename(full_file_path),
                                           xx.parent.parent.parent['targetnamespace'] if 'targetnamespace' in xx.parent.parent.parent.attrs.keys() else None,
                                               xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                               xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None
                                           ])
        df_linkbaserefs=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_linkbaserefs_Dic, df_linkbaserefs)
        del df_linkbaserefs,temp_list

    def parseRoletypes(self,dict_with_rltps,full_file_path):
        #print(f'Roletypes - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'id', 'roleuri', 'definition', 'uo_pres','uo_def', 'uo_gen']
        if dict_with_rltps:
            for xx in dict_with_rltps:
                usedon = [yy.contents[0] for yy in xx.findAll(re.compile('.*usedon$'))]
                temp_list.append([self.version,self.rinok, os.path.basename(full_file_path),
                                            xx['id'] if 'id' in xx.attrs.keys() else None,
                                            xx['roleuri'] if 'roleuri' in xx.attrs.keys() else None,
                                            xx.find(re.compile('.*definition$')).contents[0] if xx.find(re.compile('.*definition$')) else None,
                                            1 if 'link:presentationLink' in usedon else 0, \
                                            1 if 'link:definitionLink' in usedon else 0, \
                                            1 if 'gen:link' in usedon else 0])
        df_roletypes=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_roletypes_Dic, df_roletypes)
        del df_roletypes,temp_list

    def parseLabels(self,dict_with_lbls,full_file_path):
        #print(f'Labels - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'parentrole', 'type', 'label', 'role', 'title',
                                               'lang', 'id', 'text']
        if dict_with_lbls:
            for xx in dict_with_lbls:
                temp_list.append([
                    self.version,self.rinok, os.path.basename(full_file_path),
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                    xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else None,
                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                    xx['xml:lang'] if 'xml:lang' in xx.attrs.keys() else None,
                    xx['id'] if 'id' in xx.attrs.keys() else None,
                    xx.text
                ])
        df_labels=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_labels_Dic, df_labels)
        del df_labels,temp_list

    def parseLocators(self,dict_with_loc,full_file_path,tag):
        temp_list=[]
        columns=['version','rinok', 'entity', 'locfrom', 'parentrole', 'type', 'href','href_id', 'label', 'title']
        #print(f'Locators - {full_file_path}')
        if dict_with_loc:
            for xx in dict_with_loc:
                temp_list.append([
                    self.version,self.rinok, os.path.basename(full_file_path), tag,
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                    xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None,
                    xx['xlink:href'].split('#')[1] if 'xlink:href' in xx.attrs.keys() else None,
                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None])
        df_locators=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_locators_Dic, df_locators)
        del df_locators,temp_list

    def parseRolerefs(self,dict_with_rlrfs,full_file_path,tag):
        #print(f'Rolerefs - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'rolefrom', 'roleuri', 'type', 'href']
        if dict_with_rlrfs:
            for xx in dict_with_rlrfs:
                temp_list.append([self.version,self.rinok, os.path.basename(full_file_path), tag,
                                           xx['roleuri'] if 'roleuri' in xx.attrs.keys() else None,
                                           xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                           xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None
                                           ])
        df_rolerefs=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_rolerefs_Dic, df_rolerefs)
        del df_rolerefs,temp_list

    def parseTableschemas(self,dict_with_tsch,full_file_path,tag):
        #print(f'Tableschemas - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'rolefrom', 'parentrole', 'type', 'label', 'title', 'id', 'parentchildorder']
        if dict_with_tsch:
            for xx in dict_with_tsch:
                temp_list.append([self.version,self.rinok, os.path.basename(full_file_path), tag,
                                                xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                                xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else None,
                                                xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                                xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                                xx['id'] if 'id' in xx.attrs.keys() else None,
                                                xx['parentchildorder'] if 'parentchildorder' in xx.attrs.keys() else None
                                           ])
        df_tableschemas=pd.DataFrame(data=temp_list,columns=columns)
        self.appendDfs_Dic(self.df_tableschemas_Dic, df_tableschemas)
        del df_tableschemas,temp_list

    def parseArcs(self,dict_with_arcs,full_file_path,tag):
        # print(f'Arcs - {full_file_path}')
        temp_list=[]
        columns=['version','rinok', 'entity', 'arctype', 'parentrole', 'type', 'arcrole',
                                             'arcfrom', 'arcto', 'title', 'usable', 'closed', 'contextelement',
                                             'targetrole', 'order', 'preferredlabel', 'use', 'priority','complement','cover','name','axis'
                                             ]
        if dict_with_arcs:
            for arc in dict_with_arcs:
                temp_list.append([
                    self.version,self.rinok, os.path.basename(full_file_path),
                    tag,
                    arc.parent['xlink:role'] if 'xlink:role' in arc.parent.attrs.keys() else None,
                    arc['xlink:type'] if 'xlink:type' in arc.attrs.keys() else None,
                    arc['xlink:arcrole'] if 'xlink:arcrole' in arc.attrs.keys() else None,
                    arc['xlink:from'] if 'xlink:from' in arc.attrs.keys() else None,
                    arc['xlink:to'] if 'xlink:to' in arc.attrs.keys() else None,
                    arc['xlink:title'] if 'xlink:title' in arc.attrs.keys() else None,
                    arc['xbrldt:usable'] if 'xbrldt:usable' in arc.attrs.keys() else None,
                    arc['xbrldt:closed'] if 'xbrldt:closed' in arc.attrs.keys() else None,
                    arc['xbrldt:contextelement'] if 'xbrldt:contextelement' in arc.attrs.keys() else None,
                    arc['xbrldt:targetrole'] if 'xbrldt:targetrole' in arc.attrs.keys() else None,
                    arc['order'] if 'order' in arc.attrs.keys() else None,
                    arc['preferredlabel'] if 'preferredlabel' in arc.attrs.keys() else None,
                    arc['use'] if 'use' in arc.attrs.keys() else None,
                    arc['priority'] if 'priority' in arc.attrs.keys() else None,
                    arc['complement'] if 'complement' in arc.attrs.keys() else None,
                    arc['cover'] if 'cover' in arc.attrs.keys() else None,
                    arc['name'] if 'name' in arc.attrs.keys() else None,
                    arc['axis'] if 'axis' in arc.attrs.keys() else None,
                ])
        df_arcs=pd.DataFrame(data = temp_list, columns = columns)
        self.appendDfs_Dic(self.df_arcs_Dic, df_arcs)
        del temp_list

    def concatDfs(self,dfs):
        try: ret=pd.concat(dfs).reset_index(drop=True)
        except: ret=None
        return ret

    def writeThread(self, func):
        func()

    def appendDfs_Dic(self,df,df_to_append):
        append=True
        while append:
            try:
                df.append(df_to_append)
                append=False
            except:
                None

    def parsetag(self,filepath,main_tree):
        with open(filepath,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml')
        soup_root=soup.contents[2]
        soup_tree=soup_root.find(re.compile(f'.*{main_tree}$'))
        return soup_tree

