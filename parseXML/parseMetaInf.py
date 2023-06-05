import  pandas as pd,os
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
        columns=['version','nfotype','reporttype','reportperiodtype','pathtoxsd','nfotyperus','reporttyperus','reportperiodtyperus','is_exist']
        soup=self.parsetag(f'{self.path}/entry_point.xml','arrayofentrypoint')
        entrypoints=soup.find_all('entrypoint')
        for xx in entrypoints:
            if os.path.exists(xx.find('pathtoxsd').text.replace('../',f'{self.version}/')):
                is_exist=True
            else:
                is_exist=False
            temp_list.append(
                [self.version,
                xx.find('nfotype').text if xx.find('nfotype') else None ,
                xx.find('reporttype').text if xx.find('reporttype') else None,
                xx.find('reportperiodtype').text if xx.find('reportperiodtype') else None,
                xx.find('pathtoxsd').text if xx.find('pathtoxsd') else None,
                xx.find('nfotyperus').text if xx.find('nfotyperus') else None,
                xx.find('reporttyperus').text if xx.find('reporttyperus') else None,
                xx.find('reportperiodtyperus').text if xx.find('reportperiodtyperus') else None,
                is_exist
                 ]
            )
        df_entrypoints = pd.DataFrame(data=temp_list,columns=columns)
        return {'df_entrypoints':df_entrypoints}
    def parsetaxpackage(self):
        temp_list = []
        columns= ['version','tax_identifier','tax_name','tax_version','ep_name','ep_descr','ep_href','xsd','is_exist']
        soup = self.parsetag(f'{self.path}/taxonomyPackage.xml', 'tp:taxonomypackage')
        tax_identifier=soup.find('tp:identifier').text
        tax_name=soup.find('tp:name').text
        tax_version=soup.find('tp:version').text
        entrypoints=soup.find_all_next('tp:entrypoint')
        for xx in entrypoints:
            ep_name = xx.find('tp:name').text if xx.find('tp:name') else None
            ep_descr = xx.find('tp:description').text if xx.find('tp:description') else None
            if xx.find('tp:entrypointdocument'):
                ep_href = xx.find('tp:entrypointdocument')['href'] if 'href' in xx.find('tp:entrypointdocument').attrs.keys() else None
                xsd = ep_href.split('/')[-1]
                if os.path.exists(ep_href.replace('http://',f'{self.version}/')):
                    is_exist=True
                else:
                    is_exist=False
            temp_list.append([self.version,tax_identifier,tax_name,tax_version,ep_name,ep_descr,ep_href,xsd,is_exist])
        df_taxpackage=pd.DataFrame(data=temp_list,columns=columns)
        return {'df_taxpackage':df_taxpackage}

    def parsecatalog(self):
        temp_list = []
        columns = ['version','xmlns','uristart','prefix','is_exist']
        soup = self.parsetag(f'{self.path}/catalog.xml', 'catalog')
        xmlns = soup['xmlns']
        tags=soup.find_all('rewriteuri')
        for xx in tags:
            if 'rewriteprefix' in xx.attrs.keys():
                prefix=xx['rewriteprefix']
                if os.path.isdir(prefix.replace('../',f'{self.version}/')):
                    is_exist=True
                else:
                    is_exist=False
            else:
                prefix=None
                is_exist=False
            uristart = xx['uristartstring'] if 'uristartstring' in xx.attrs.keys() else None
            temp_list.append([self.version,xmlns,uristart,prefix,is_exist])
        df_catalog=pd.DataFrame(data=temp_list,columns=columns)
        return {'df_catalog':df_catalog}

if __name__ == "__main__":
    ss=c_parseMeta('final_5_2')
    zz=ss.parsecatalog()
    None