import gc
import os,re

import pandas as pd
from bs4 import BeautifulSoup
import warnings
import parseToDf
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool


class c_parseFormula():
    def __init__(self,taxonomy,rinok_folder,rinok):
        self.rinok=rinok
        self.path_tax=f'{os.getcwd()}\\{taxonomy}\\'
        self.path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\rep\\2023-03-31\\'
        self.df=parseToDf.c_parseToDf(rinok)

    def parsesupport(self):
        path_supp = self.path_folder + '\\ep\\'
        for xx in os.listdir(path_supp):
            if 'support' in xx:
                support_file = xx
        path_file = path_supp + support_file
        with open(path_file, 'rb') as f:
            ff = f.read()
        soup = BeautifulSoup(ff, 'lxml').contents[2].find_next('xsd:schema')
        namesps = soup.find_all('xsd:import')
        for xx in namesps:
            if 'www.cbr.ru' in xx['namespace']:
                self.df.df_tables.loc[-1] = [self.rinok, os.path.basename(path_file),
                                             soup['targetnamespace'], xx['schemalocation'], xx['namespace']]
                self.df.df_tables.index = self.df.df_tables.index + 1
                self.df.df_tables = self.df.df_tables.sort_index()

    def parseformula(self):
        tabs = [[row['schemalocation'], row['namespace']] for index, row in self.df.df_tables.iterrows()]
        # for xx in range(len(tabs)):
        #     print(xx,tabs[xx])
        schemalocationnamespace=[tabs[26][0],tabs[26][1]]
        print(schemalocationnamespace)
        tab_temp = f"{schemalocationnamespace[1]}\\"
        schema_temp = schemalocationnamespace[0]
        schema_temp_2 = schema_temp.split('/')[-1]
        path_xsd = self.path_tax + tab_temp.replace('http://', '') + schema_temp_2
        soup = self.df.parsetag(path_xsd, 'xsd:schema') if self.df.parsetag(path_xsd,
                                                                            'xsd:schema') != None else self.df.parsetag(
            path_xsd, 'xs:schema')

        self.df.parseRoletypes(soup.find_all('link:roletype'), path_xsd)
        self.df.parseLinkbaserefs(soup.find_all('link:linkbaseref'), path_xsd, 'maintabxsd')
        linkbaserefs = soup.find_all('link:linkbaseref')
        formulas = [f"{self.path_tax}{tab_temp.replace('http://', '')}{yy['xlink:href']}" for yy in linkbaserefs if
                re.findall(r'formula\S*.xml', yy['xlink:href'])]
        if formulas:
            for path in formulas:
                if self.df.parsetag(path, 'link:linkbase'): replace_ = ''
                else: replace_ = 'link:'
                soup_formula = self.df.parsetag(path, 'link:linkbase'.replace(replace_, ''))

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
                t_all=[t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11]
                with ThreadPool(processes=11) as pool:
                    pool.map(self.df.writeThread, t_all)

    def startParse(self):
        ss.parsesupport()
        ss.parseformula()


if __name__ == "__main__":
    ss=c_parseFormula('final_5_2','npf','npf')
    ss.parsesupport()
    ss.parseformula()
    None