import os
from bs4 import BeautifulSoup
import warnings
import parseToDf
warnings.filterwarnings("ignore")

class c_parseFormula():
    def __init__(self,taxonomy,rinok_folder,rinok):
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

    def parsetab(self):
        for index, row in self.df.df_tables.iterrows():
            # if 'sr_0420501' in row['schemalocation']:
            #     print(row['schemalocation'])
            if row['schemalocation']:
                tab_temp=f"{row['namespace']}\\"
                schema_temp=row['schemalocation']
                schema_temp_2=schema_temp.split('/')[-1]
                path_xsd=self.path_tax+tab_temp.replace('http://','')+schema_temp_2
                soup=self.df.parsetag(path_xsd,'xsd:schema') if self.df.parsetag(path_xsd,'xsd:schema')!=None else self.df.parsetag(path_xsd,'xs:schema')

                self.df.parseRoletypes(soup.find_all('link:roletype'),path_xsd)
                self.df.parseLinkbaserefs(soup.find_all('link:linkbaseref'),path_xsd,'maintabxsd')

                linkbaserefs = soup.find_all('link:linkbaseref')
                for yy in linkbaserefs:
                    if 'definition.xml' in yy['xlink:href']:
                        path_def=self.path_tax+tab_temp.replace('http://','')+yy['xlink:href']
                        soup_def=self.df.parsetag(path_def,'link:linkbase')
                        self.df.parseRolerefs(soup_def.find_all('link:roleref'),path_def,'definition')
                        self.df.parseLocators(soup_def.find_all('link:loc'),path_def,'definition')
                        self.df.parseArcs(soup_def.find_all('link:definitionarc'),path_def,'definition')

                    if 'presentation.xml' in yy['xlink:href']:
                        path_pres=self.path_tax+tab_temp.replace('http://','')+yy['xlink:href']
                        if self.df.parsetag(path_pres,'link:linkbase'):
                            soup_pres=self.df.parsetag(path_pres,'link:linkbase')
                            self.df.parseRolerefs(soup_pres.find_all('link:roleref'),path_pres,'presentation')
                            self.df.parseLocators(soup_pres.find_all('link:loc'),path_pres,'presentation')
                            self.df.parseArcs(soup_pres.find_all('link:presentationarc'),path_pres,'presentation')
                        else:
                            soup_pres = self.df.parsetag(path_pres, 'linkbase')
                            self.df.parseRolerefs(soup_pres.find_all('roleref'), path_pres, 'presentation')
                            self.df.parseLocators(soup_pres.find_all('loc'), path_pres, 'presentation')
                            self.df.parseArcs(soup_pres.find_all('presentationarc'), path_pres, 'presentation')

                    if 'lab.xml' in yy['xlink:href'] or 'rend.xml' in yy['xlink:href']:
                        path_labrend=f"{self.path_tax}{tab_temp.replace('http://','')}{yy['xlink:href']}"
                        soup_labrend=self.df.parsetag(path_labrend,'linkbase')
                        if 'lab' in yy['xlink:href']:
                            self.parselab(soup_labrend,path_labrend)
                        elif 'rend' in yy['xlink:href']:
                            self.parserend(soup_labrend,path_labrend)


    def parselab(self,soup,path):
        self.df.parseRolerefs(soup.find_all('roleref'),path,'lab')
        self.df.parseLocators(soup.find_all('loc'),path,'lab')
        self.df.parseLabels(soup.find_all('label:label'),path)
        self.df.parseArcs(soup.find_all('gen:arc'),path,'gen:arc')

    def parserend(self,soup,path):
        self.df.parseRolerefs(soup.find_all('roleref'),path,'rend')
        self.df.parseTableschemas(soup.find_all('table:table'),path,'table')
        self.df.parseTableschemas(soup.find_all('table:breakdown'),path,'breakdown')
        self.df.parseArcs(soup.find_all('table:tablebreakdownarc'),path,'table:tablebreakdownarc')
        self.df.parseArcs(soup.find_all_next('table:definitionnodesubtreearc'),path,'table:definitionnodesubtreearc')

    def startParse(self):
        self.parsesupport()
        self.parsetab()
        self.parsenosupport()

if __name__ == "__main__":
    ss=c_parseFormula('final_5_2','purcb','purcb')
    ss.parsenosupport()
    ss.parsesupport()
    ss.parsetab()
    None