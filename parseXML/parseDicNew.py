import os,gc
import parseToDf
import warnings
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseDic():
    def __init__(self,taxonomy,rinok_folder,rinok):
        self.df = parseToDf.c_parseToDf(rinok)
        if rinok_folder in ('udr\\dim','udr\\dom'):
            path_folder=f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\'
            self.path_dic = f'{path_folder}{rinok}-int.xsd'
            self.path_label = f'{path_folder}{rinok}-int-label.xml'
            self.path_definition = f'{path_folder}{rinok}-dic-definition.xml'
            self.path_pres = f'{path_folder}{rinok}-dic-presentation.xml'
            self.rinok = rinok
            self.rinok_folder = rinok_folder
        else:
            self.rinok_folder=rinok_folder
            self.rinok=rinok
            path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\dic\\'
            self.path_dic=f'{path_folder}{rinok}-dic.xsd'
            self.path_label=f'{path_folder}{rinok}-dic-label.xml'
            self.path_definition=f'{path_folder}{rinok}-dic-definition.xml'
            self.path_pres=f'{path_folder}{rinok}-dic-presentation.xml'
    def writeThread(self, func):
        func()

    def parseDic(self):
        soup_dic = self.df.parsetag(self.path_dic, 'xsd:appinfo')
        soup_label = self.df.parsetag(self.path_label, 'link:linkbase')
        try: soup_def = self.df.parsetag(self.path_definition, 'link:linkbase')
        except: soup_def = None
        try: soup_pres=self.df.parsetag(self.path_pres,'link:linkbase')
        except: soup_pres = None

        def t1(): self.df.parseRoletypes(soup_dic.find_all('link:roletype'),self.path_dic)
        def t2(): self.df.parseElements(soup_dic.find_all_next('xsd:element'),self.path_dic)
        def t3(): self.df.parseLocators(soup_label.find_all('link:loc'),self.path_label,'label')
        def t4(): self.df.parseLabels(soup_label.find_all('link:label'), self.path_label)
        def t5(): self.df.parseArcs(soup_label.find_all('link:labelarc'),self.path_label,'label')
        def t6(): self.df.parseRolerefs(soup_def.find_all('link:roleref') if soup_def else None,self.path_definition,'definition')
        def t7(): self.df.parseLocators(soup_def.find_all_next('link:loc') if soup_def else None, self.path_definition, 'definition')
        def t8(): self.df.parseArcs(soup_def.find_all_next('link:definitionarc') if soup_def else None, self.path_label, 'definition')
        def t9(): self.df.parseRolerefs(soup_pres.find_all('link:roleref') if soup_pres else None, self.path_definition, 'presentation')
        def t10(): self.df.parseLocators(soup_pres.find_all_next('link:loc') if soup_pres else None, self.path_definition, 'presentation')
        def t11(): self.df.parseArcs(soup_pres.find_all_next('link:presentationarc') if soup_pres else None, self.path_label, 'presentation')
        defs=[t1,t2,t3,t4,t5]
        defs2 = [t6, t7, t8]
        defs3=[t9,t10,t11]
        with ThreadPool(processes=5) as pool:
            pool.map(self.writeThread, defs)
        with ThreadPool(processes=3) as pool:
            pool.map(self.writeThread, defs2)
        with ThreadPool(processes=3) as pool:
            pool.map(self.writeThread, defs3)

        del soup_dic,soup_label,soup_def,soup_pres
        gc.collect()

    def startParse(self):
        self.parseDic()

if __name__ == "__main__":
    ss=c_parseDic('final_5_2','purcb','purcb')
    ss.parseDic()

    None
