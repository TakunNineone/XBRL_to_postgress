import gc
import os,re

import pandas as pd
from bs4 import BeautifulSoup
import warnings
import parseToDf
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseTab():
    def __init__(self,taxonomy,rinok_folder,rinok):
        if rinok_folder=='bfo':
            self.rinok = 'bfo'
            self.path_tax = f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\rep\\2023-03-31\\'
            self.df = parseToDf.c_parseToDf(rinok)
        else:
            self.rinok=rinok
            self.path_tax=f'{os.getcwd()}\\{taxonomy}\\'
            self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\rep\\2023-03-31\\'
            self.df=parseToDf.c_parseToDf(rinok)

    def parsesupport(self):
        path_supp=self.path_folder+'\\ep\\'
        for xx in os.listdir(path_supp):
            if  'support' in xx:
                support_file=xx
        path_file = path_supp + support_file
        with open(path_file,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml').contents[2].find_next('xsd:schema')
        namesps=soup.find_all('xsd:import')
        for xx in namesps:
            if 'www.cbr.ru' in xx['namespace']:
                self.df.df_tables.loc[-1] = [self.rinok, os.path.basename(path_file),
                                          soup['targetnamespace'],xx['schemalocation'],xx['namespace']]
                self.df.df_tables.index = self.df.df_tables.index + 1
                self.df.df_tables = self.df.df_tables.sort_index()

    def parsenosupport(self):
        path_supp = self.path_folder + '\\ep\\'
        for ep in os.listdir(path_supp):
            if 'support' not in ep:
                path_file = path_supp + ep
                with open(path_file, 'rb') as f:
                    ff = f.read()
                soup = BeautifulSoup(ff, 'lxml').contents[1].find_next('xsd:schema')
                namesps = soup.find_all('xsd:import')
                for xx in namesps:
                    if 'www.cbr.ru' in xx['namespace']:
                        self.df.df_tables.loc[-1] = [self.rinok, os.path.basename(path_file),
                                                     soup['targetnamespace'], xx['schemalocation'], xx['namespace']]
                        self.df.df_tables.index = self.df.df_tables.index + 1
                        self.df.df_tables = self.df.df_tables.sort_index()
        None

    def parsetabThread(self):
        tabs=[[row['schemalocation'],row['namespace']] for index, row in self.df.df_tables.iterrows()]
        with ThreadPool(processes=60) as pool:
            pool.map(self.parsetab, tabs)

    def parsetab(self,schemalocationnamespace):
        tab_temp=f"{schemalocationnamespace[1]}\\"
        schema_temp=schemalocationnamespace[0]
        schema_temp_2=schema_temp.split('/')[-1]
        path_xsd=self.path_tax+tab_temp.replace('http://','')+schema_temp_2
        soup=self.df.parsetag(path_xsd,'xsd:schema') if self.df.parsetag(path_xsd,'xsd:schema')!=None else self.df.parsetag(path_xsd,'xs:schema') if self.df.parsetag(path_xsd,'xs:schema') else self.df.parsetag(path_xsd,'schema')

        self.df.parseRoletypes(soup.find_all('link:roletype'),path_xsd)
        self.df.parseLinkbaserefs(soup.find_all('link:linkbaseref'),path_xsd,'maintabxsd')

        linkbaserefs = soup.find_all('link:linkbaseref')

        formulas = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                re.findall(r'formula\S*.xml', yy['xlink:href'])]
        if formulas:
            for path in formulas:
                if self.df.parsetag(path, 'link:linkbase'): replace_ = ''
                else: replace_ = 'link:'
                soup_formula = self.df.parsetag(path, 'link:linkbase'.replace(replace_, ''))
                def t0(): self.df.parse_generals(soup_formula.find_all_next('gf:general'),path)
                def t1(): self.df.parseRolerefs(soup_formula.find_all('link:roleref'),path,'formula')
                def t2(): self.df.parseArcs(soup_formula.find_all_next('variable:variablearc'),path,'formula')
                def t3(): self.df.parseArcs(soup_formula.find_all_next('variable:variablefilterarc'), path, 'formula')
                def t4(): self.df.parseArcs(soup_formula.find_all_next('gen:arc'), path, 'formula')
                def t5(): self.df.parseLocators(soup_formula.find_all_next('link:loc'),path,'formula')
                def t6(): self.df.parseLabels(soup_formula.find_all_next('msg:message'),path)
                def t7(): self.df.parse_assertions(soup_formula,path)
                def t8(): self.df.parse_concepts(soup_formula,path)
                def t9(): self.df.parse_factvars(soup_formula,path)
                def t10(): self.df.parse_tdimensions(soup_formula,path)
                def t11(): self.df.parse_edimensions(soup_formula,path)
                t_all=[t0,t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11]
                with ThreadPool(processes=11) as pool:
                   pool.map(self.df.writeThread, t_all)
        rend = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                re.findall(r'rend\S*.xml',yy['xlink:href'])]
        if rend:
            with ThreadPool(processes=10) as pool:
                pool.map(self.df.parseRulenodes, rend)
            with ThreadPool(processes=10) as pool:
                pool.map(self.parserend, rend)

        pres = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if re.findall(r'presentation\S*.xml',yy['xlink:href'])]
        if pres:
            with ThreadPool(processes=len(pres)) as pool:
                pool.map(self.parsepres, pres)

        defin = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if re.findall(r'definition\S*.xml',yy['xlink:href'])]
        if defin:
            with ThreadPool(processes=len(defin)) as pool:
                pool.map(self.parsedef, defin)



        lab = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
               re.findall(r'lab\S*.xml',yy['xlink:href'])]
        if lab:
            with ThreadPool(processes=20) as pool:
                pool.map(self.parselab, lab)

    def parsedef(self,path):
        if self.df.parsetag(path,'link:linkbase'): replace_=''
        else: replace_='link:'
        soup_pres=self.df.parsetag(path,'link:linkbase'.replace(replace_,''))
        def t1():self.df.parseRolerefs(soup_pres.find_all('link:roleref'.replace(replace_,'')),path,'definition')
        def t2():self.df.parseLocators(soup_pres.find_all('link:loc'.replace(replace_,'')),path,'definition')
        def t3():self.df.parseArcs(soup_pres.find_all('link:definitionarc'.replace(replace_,'')),path,'definition')
        t_all=[t1,t2,t3]
        with ThreadPool(processes=3) as pool:
            pool.map(self.df.writeThread, t_all)

    def parsepres(self,path):
        if self.df.parsetag(path,'link:linkbase'): replace_=''
        else: replace_='link:'
        soup_pres=self.df.parsetag(path,'link:linkbase'.replace(replace_,''))
        def t1():self.df.parseRolerefs(soup_pres.find_all('link:roleref'.replace(replace_,'')),path,'presentation')
        def t2():self.df.parseLocators(soup_pres.find_all('link:loc'.replace(replace_,'')),path,'presentation')
        def t3():self.df.parseArcs(soup_pres.find_all('link:presentationarc'.replace(replace_,'')),path,'presentation')
        t_all=[t1,t2,t3]
        with ThreadPool(processes=3) as pool:
            pool.map(self.df.writeThread, t_all)

    def parselab(self,path):
        if self.df.parsetag(path, 'linkbase'):
            soup = self.df.parsetag(path, 'linkbase')
        else:
            soup = self.df.parsetag(path, 'link:linkbase')
        def t1(): self.df.parseRolerefs(soup.find_all('roleref'),path,'lab')
        def t2(): self.df.parseLocators(soup.find_all('loc'),path,'lab')
        def t3(): self.df.parseLabels(soup.find_all('label:label'),path)
        def t4(): self.df.parseArcs(soup.find_all('gen:arc'),path,'gen:arc')
        t_all=[t1, t2, t3, t4]
        with ThreadPool(processes=4) as pool:
            pool.map(self.df.writeThread, t_all)

    def parserend(self,path):
        if self.df.parsetag(path, 'linkbase'):
            soup = self.df.parsetag(path, 'linkbase')
        else:
            soup = self.df.parsetag(path, 'link:linkbase')
        def t1():self.df.parseRolerefs(soup.find_all('roleref'),path,'rend')
        def t2():self.df.parseTableschemas(soup.find_all('table:table'),path,'table')
        def t3():self.df.parseTableschemas(soup.find_all('table:breakdown'),path,'breakdown')
        def t4():self.df.parseArcs(soup.find_all('table:tablebreakdownarc'),path,'table:tablebreakdownarc')
        def t5():self.df.parseArcs(soup.find_all_next('table:definitionnodesubtreearc'),path,'table:definitionnodesubtreearc')
        t_all = [t1, t2, t3, t4, t5]
        with ThreadPool(processes=5) as pool:
            pool.map(self.df.writeThread, t_all)

    def startParse(self):
        self.parsesupport()
        self.parsetabThread()
        self.parsenosupport()
        gc.collect()
        return {
            'df_rulenodes':self.df.concatDfs(self.df.df_rulenodes_Dic),
                'df_rulenodes_c':self.df.concatDfs(self.df.df_rulenodes_c_Dic),
                'df_rulenodes_p':self.df.concatDfs(self.df.df_rulenodes_p_Dic),
                'df_rulenodes_e':self.df.concatDfs(self.df.df_rulenodes_e_Dic),
                'df_roletypes':self.df.concatDfs(self.df.df_roletypes_Dic),
                'df_locators':self.df.concatDfs(self.df.df_locators_Dic),
                'df_arcs':self.df.concatDfs(self.df.df_arcs_Dic),
                'df_labels':self.df.concatDfs(self.df.df_labels_Dic),
                'df_rolerefs':self.df.concatDfs(self.df.df_rolerefs_Dic),
                'df_tableschemas':self.df.concatDfs(self.df.df_tableschemas_Dic),
                'df_linkbaserefs':self.df.concatDfs(self.df.df_linkbaserefs_Dic),
                'df_tables': self.df.df_tables,
                'df_va_edmembers':self.df.concatDfs(self.df.df_va_edmembers_Dic),
                'df_va_edimensions':self.df.concatDfs(self.df.df_va_edimensions_Dic),
                'df_va_tdimensions':self.df.concatDfs(self.df.df_va_tdimensions_Dic),
                'df_va_concepts':self.df.concatDfs(self.df.df_va_concepts_Dic),
                'df_va_factvars':self.df.concatDfs(self.df.df_va_factvars_Dic),
                'df_va_assertions':self.df.concatDfs(self.df.df_va_assertions_Dic),
                'df_va_generals': self.df.concatDfs(self.df.df_va_generals_Dic)
                }


if __name__ == "__main__":
    ss=c_parseTab('final_5_2','npf','npf')
    #tables=ss.startParse()
    ss.parsesupport()
    ss.parsetab(['../tab/sr_0420256/sr_0420256.xsd', 'http://www.cbr.ru/xbrl/nso/npf/rep/2023-03-31/tab/sr_0420256'])
    None