import gc
import os,re

import pandas as pd
from bs4 import BeautifulSoup
import warnings
import parseToDf
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseTab():
    def __init__(self,taxonomy,rinok_folder,rinok,period):
        if rinok_folder=='bfo':
            self.rinok = 'bfo'
            self.version=taxonomy
            self.period=period
            self.path_tax = f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\rep\\{period}\\'
            self.df = parseToDf.c_parseToDf(taxonomy,rinok)
        else:
            self.rinok=rinok
            self.version = taxonomy
            self.period = period
            self.path_tax=f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\rep\\{period}\\'
            self.df=parseToDf.c_parseToDf(taxonomy,rinok)

    def parsesupport(self):
        path_supp=self.path_folder+'\\ep\\'
        if os.path.isdir(path_supp):
            for xx in os.listdir(path_supp):
                if  'support' in xx:
                    support_file=xx
            path_file = path_supp + support_file
            with open(path_file,'rb') as f:
                ff=f.read()
            soup=BeautifulSoup(ff,'lxml').contents[2].find_next(re.compile('.*schema$'))
            namesps=soup.find_all(re.compile('.*import$'))
            for xx in namesps:
                if 'www.cbr.ru' in xx['namespace'] and f'{self.period}/tab' in xx['namespace']:
                    self.df.df_tables.loc[-1] = [self.version,self.rinok, os.path.basename(path_file),
                                              soup['targetnamespace'],xx['schemalocation'],xx['namespace']]
                    self.df.df_tables.index = self.df.df_tables.index + 1
                    self.df.df_tables = self.df.df_tables.sort_index()
        else:
            None

    def parsenosupport(self):
        path_supp = self.path_folder + '\\ep\\'
        if os.path.isdir(path_supp):
            for ep in os.listdir(path_supp):
                if 'support' not in ep:
                    path_file = path_supp + ep
                    with open(path_file, 'rb') as f:
                        ff = f.read()
                    soup = BeautifulSoup(ff, 'lxml').contents[1].find_next(re.compile('.*schema$'))
                    namesps = soup.find_all(re.compile('.*import$'))
                    for xx in namesps:
                        if 'www.cbr.ru' in xx['namespace'] and f'{self.period}/tab' in xx['namespace']:
                            self.df.df_tables.loc[-1] = [self.version, self.rinok, os.path.basename(path_file),
                                                         soup['targetnamespace'], xx['schemalocation'], xx['namespace']]
                            self.df.df_tables.index = self.df.df_tables.index + 1
                            self.df.df_tables = self.df.df_tables.sort_index()
            None
        else:
            None

    def parsetabThread(self):
        tabs=[[row['schemalocation'],row['namespace']] for index, row in self.df.df_tables.iterrows()]

        if tabs:
            with ThreadPool(processes=60) as pool:
                pool.map(self.parsetab, tabs)

    def parsetab(self,schemalocationnamespace):
        # print(schemalocationnamespace)

        tab_temp=f"{schemalocationnamespace[1]}\\"
        schema_temp=schemalocationnamespace[0]
        schema_temp_2=schema_temp.split('/')[-1]
        path_xsd=self.path_tax+tab_temp.replace('http://','')+schema_temp_2
        soup=self.df.parsetag(path_xsd,'schema')

        self.df.parseRoletypes(soup.find_all(re.compile('.*roletype$')),path_xsd)
        self.df.parseLinkbaserefs(soup,path_xsd)
        self.df.parseTableParts(soup,path_xsd)
        gc.collect()

        linkbaserefs = soup.find_all(re.compile('.*linkbaseref$'))
        formulas = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                re.findall(r'formula\S*.xml', yy['xlink:href'])]

        if formulas:
            for path in formulas:
                soup_formula = self.df.parsetag(path,'linkbase')
                def t0(): self.df.parse_generals(soup_formula.find_all_next(re.compile('.*generalvariable$')),path)
                def t1(): self.df.parseRolerefs(soup_formula.find_all(re.compile('.*roleref$')),path,'formula')
                def t2(): self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablearc$')),path,'formula')
                def t3(): self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablefilterarc$')), path, 'formula')
                def t3_2(): self.df.parseArcs(soup_formula.find_all_next(re.compile('.*variablesetfilterarc$')), path, 'formula')
                def t4(): self.df.parseArcs(soup_formula.find_all_next(re.compile(r'^.{0,4}arc$')), path, 'formula')
                def t5(): self.df.parseLocators(soup_formula.find_all_next(re.compile(r'^.{0,5}loc$')),path,'formula')
                def t6(): self.df.parseLabels(soup_formula.find_all_next(re.compile('.*message$')),path)
                def t7(): self.df.parse_assertions(soup_formula.find_all_next(re.compile('.*valueassertion$')),path,'valueassertion'),
                def t7_2(): self.df.parse_assertions(soup_formula.find_all_next(re.compile('.*existenceassertion$')), path, 'existenceassertion')
                def t8(): self.df.parse_concepts(soup_formula,path)
                def t9(): self.df.parse_factvars(soup_formula,path)
                def t10(): self.df.parse_tdimensions(soup_formula,path)
                def t11(): self.df.parse_edimensions(soup_formula,path)
                def t12(): self.df.parse_aspectcovers(soup_formula.find_all_next(re.compile('.*aspectcover$')),path)
                def t13(): self.df.parse_assertionset(soup_formula,path)
                def t14(): self.df.parse_precond(soup_formula,path)
                def t15(): self.df.parse_messages(soup_formula,path)
                t_all=[t0,t1,t2,t3,t3_2,t4,t5,t6,t7,t7_2,t8,t9,t10,t11,t12,t13,t14,t15]

                with ThreadPool(processes=18) as pool:
                   pool.map(self.df.writeThread, t_all)
        gc.collect()

        rend = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                re.findall(r'rend\S*.xml',yy['xlink:href'])]
        if rend:
            with ThreadPool(processes=10) as pool:
                pool.map(self.df.parseRulenodes,rend)
            with ThreadPool(processes=10) as pool:
                pool.map(self.df.parseRulesets,rend)
            with ThreadPool(processes=10) as pool:
                pool.map(self.df.parseAspectnodes,rend)
            with ThreadPool(processes=10) as pool:
                pool.map(self.parserend, rend)
        gc.collect()

        pres = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if re.findall(r'presentation\S*.xml',yy['xlink:href'])]
        if pres:
            with ThreadPool(processes=len(pres)) as pool:
                pool.map(self.parsepres, pres)
        gc.collect()

        defin = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if re.findall(r'definition\S*.xml',yy['xlink:href'])]
        if defin:
            with ThreadPool(processes=len(defin)) as pool:
                pool.map(self.parsedef, defin)
        gc.collect()

        lab = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
               re.findall(r'lab\S*.xml',yy['xlink:href'])]
        if lab:
            with ThreadPool(processes=20) as pool:
                pool.map(self.parselab, lab)
        gc.collect()

    def parsedef(self,path):
        soup_pres=self.df.parsetag(path,'linkbase')
        def t1():self.df.parseRolerefs(soup_pres.find_all(re.compile('.*roleref$')),path,'definition')
        def t2():self.df.parseLocators(soup_pres.find_all(re.compile('.*loc$')),path,'definition')
        def t3():self.df.parseArcs(soup_pres.find_all(re.compile('.*definitionarc$')),path,'definition')
        t_all=[t1,t2,t3]
        with ThreadPool(processes=3) as pool:
            pool.map(self.df.writeThread, t_all)
        gc.collect()

    def parsepres(self,path):
        soup_pres=self.df.parsetag(path,'linkbase')
        def t1():self.df.parseRolerefs(soup_pres.find_all(re.compile('.*roleref$')),path,'presentation')
        def t2():self.df.parseLocators(soup_pres.find_all(re.compile('.*loc$')),path,'presentation')
        def t3():self.df.parseArcs(soup_pres.find_all(re.compile('.*presentationarc$')),path,'presentation')
        t_all=[t1,t2,t3]
        with ThreadPool(processes=3) as pool:
            pool.map(self.df.writeThread, t_all)
        gc.collect()

    def parselab(self,path):
        soup = self.df.parsetag(path, 'linkbase')
        def t1(): self.df.parseRolerefs(soup.find_all(re.compile('.*roleref$')),path,'lab')
        def t2(): self.df.parseLocators(soup.find_all(re.compile('.*loc$')),path,'lab')
        def t3(): self.df.parseLabels(soup.find_all(re.compile('.*label$')),path)
        def t4(): self.df.parseArcs(soup.find_all(re.compile('.*arc$')),path,'gen:arc')
        t_all=[t1, t2, t3, t4]
        with ThreadPool(processes=4) as pool:
            pool.map(self.df.writeThread, t_all)
        gc.collect()

    def parserend(self,path):
        soup = self.df.parsetag(path, 'linkbase')
        def t1():self.df.parseRolerefs(soup.find_all(re.compile('.*roleref$')),path,'rend')
        def t2():self.df.parseTableschemas(soup.find_all(re.compile('.*table$')),path,'table')
        def t3():self.df.parseTableschemas(soup.find_all(re.compile('.*breakdown$')),path,'breakdown')
        def t4():self.df.parseArcs(soup.find_all(re.compile('.*tablebreakdownarc$')),path,'table:tablebreakdownarc')
        def t5():self.df.parseArcs(soup.find_all_next(re.compile('.*definitionnodesubtreearc$')),path,'table:definitionnodesubtreearc')
        def t6():self.df.parseArcs(soup.find_all_next(re.compile('.*aspectnodefilterarc$')), path,'table:aspectnodefilterarc')
        def t7():self.df.parseArcs(soup.find_all_next(re.compile('.*breakdowntreearc$')), path, 'table:breakdowntreearc')
        def t8():self.df.parse_edimensions_rend(soup,path)
        t_all = [t1, t2, t3, t4, t5, t6, t7, t8]
        with ThreadPool(processes=8) as pool:
            pool.map(self.df.writeThread, t_all)
        gc.collect()

    def startParse(self):
        self.parsesupport()
        self.parsetabThread()
        self.parsenosupport()
        gc.collect()
        return {
                'df_rulenodes':self.df.concatDfs(self.df.df_rulenodes_Dic),
                'df_aspectnodes': self.df.concatDfs(self.df.df_aspectnodes_Dic),
                'df_rulenodes_c':self.df.concatDfs(self.df.df_rulenodes_c_Dic),
                'df_rulenodes_p':self.df.concatDfs(self.df.df_rulenodes_p_Dic),
                'df_rulenodes_e':self.df.concatDfs(self.df.df_rulenodes_e_Dic),
                'df_rulesets':self.df.concatDfs(self.df.df_rulesets_Dic),
                'df_rend_edmembers':self.df.concatDfs(self.df.df_rend_edmembers_Dic),
                'df_rend_edimensions':self.df.concatDfs(self.df.df_rend_edimensions_Dic),
                'df_roletypes':self.df.concatDfs(self.df.df_roletypes_Dic),
                'df_locators':self.df.concatDfs(self.df.df_locators_Dic),
                'df_arcs':self.df.concatDfs(self.df.df_arcs_Dic),
                'df_labels':self.df.concatDfs(self.df.df_labels_Dic),
                'df_rolerefs':self.df.concatDfs(self.df.df_rolerefs_Dic),
                'df_tableschemas':self.df.concatDfs(self.df.df_tableschemas_Dic),
                'df_linkbaserefs':self.df.concatDfs(self.df.df_linkbaserefs_Dic),
                'df_tables': self.df.df_tables,
                'df_tableparts': self.df.concatDfs(self.df.df_tableparts_Dic),
                'df_va_edmembers':self.df.concatDfs(self.df.df_va_edmembers_Dic),
                'df_va_edimensions':self.df.concatDfs(self.df.df_va_edimensions_Dic),
                'df_va_tdimensions':self.df.concatDfs(self.df.df_va_tdimensions_Dic),
                'df_va_concepts':self.df.concatDfs(self.df.df_va_concepts_Dic),
                'df_va_factvars':self.df.concatDfs(self.df.df_va_factvars_Dic),
                'df_va_assertions':self.df.concatDfs(self.df.df_va_assertions_Dic),
                'df_va_generals': self.df.concatDfs(self.df.df_va_generals_Dic),
                'df_va_aspectcovers': self.df.concatDfs(self.df.df_va_aspectcovers_Dic),
                'df_va_assertionsets': self.df.concatDfs(self.df.df_va_assertionset_Dic),
                'df_preconditions': self.df.concatDfs(self.df.df_preconditions_Dic),
                'df_messages': self.df.concatDfs(self.df.df_messages_Dic)
                }

if __name__ == "__main__":
    ss=c_parseTab('final_5_2','purcb','purcb','2023-03-31')
   # tables=ss.startParse()

    ss.parsetab(['../tab/SR_0420312/SR_0420312.xsd', 'http://www.cbr.ru/xbrl/nso/purcb/rep/2023-03-31/tab/SR_0420312'])
    None