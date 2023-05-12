import  pandas as pd
from bs4 import BeautifulSoup

class c_parseMeta():
    def __init__(self,taxonomy):
        self.version=taxonomy
        self.path=f'{taxonomy}/META-INF/'

    def parsetag(self,filepath,main_tree):
        with open(filepath,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml')
        soup_root=soup.contents[1]
        soup_tree=soup_root.find_next(main_tree)
        return soup_tree

    def parseentry(self):
        temp_list=[]
        columns=['version','nfotype','reporttype','reportperiodtype','pathtoxsd','nfotyperus','reporttyperus','reportperiodtyperus']
        soup=self.parsetag(f'{self.path}/entry_point.xml','arrayofentrypoint')
        entrypoints=soup.find_all('entrypoint')
        for xx in entrypoints:
            temp_list.append(
                [self.version,
                xx.find_next('nfotype').text,
                xx.find_next('reporttype').text,
                xx.find_next('reportperiodtype').text,
                xx.find_next('pathtoxsd').text,
                xx.find_next('nfotyperus').text,
                xx.find_next('reporttyperus').text,
                xx.find_next('reportperiodtyperus').text
                 ]
            )
        df_entrypoints = pd.DataFrame(data=temp_list,columns=columns)
        return {'df_entrypoints':df_entrypoints}

if __name__ == "__main__":
    ss=c_parseMeta('final_5_2')
    ss.parseentry()