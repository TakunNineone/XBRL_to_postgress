import os,gc
import parseToDf
import warnings
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseIFRS_FULL():
    def __init__(self,taxonomy):
        path_folder = f'{os.getcwd()}\{taxonomy}\\xbrl.ifrs.org\\taxonomy\\2021-03-24\\full_ifrs\\'
        # path_folder= f'{os.getcwd()}\{taxonomy}\\xbrl.ifrs.org\\taxonomy\\2015-03-11\\full_ifrs\\'
        # self.path_dic = f'{path_folder}full_ifrs-cor_2015-03-11.xsd'
        self.path_dic = f'{path_folder}full_ifrs-cor_2021-03-24.xsd'
        self.rinok = 'ifrs-full'
        self.df = parseToDf.c_parseToDf(taxonomy,self.rinok)

    def parseDic(self):
        soup_dic = self.df.parsetag(self.path_dic, 'xsd:schema')
        def t1(): self.df.parseElements(soup_dic.find_all_next('xsd:element'),self.path_dic)
        defs=[t1]
        with ThreadPool(processes=11) as pool:
            pool.map(self.df.writeThread, defs)

        del soup_dic
        gc.collect()

    def startParse(self):
        self.parseDic()
        return {'df_elements': self.df.concatDfs(self.df.df_elements_Dic)}

if __name__ == "__main__":
    ss=c_parseIFRS_FULL('final_4_3')
    dfs=ss.startParse()

    None
