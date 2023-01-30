import os, pandas as pd,gc
from bs4 import BeautifulSoup
import warnings
warnings.filterwarnings("ignore")
from multiprocessing.pool import ThreadPool

class c_parseDic():
    def __init__(self,taxonomy,rinok_folder,rinok):
        if rinok_folder in ('udr\\dim','udr\\dom'):
            path_folder=f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\{rinok_folder}\\'
            self.path_dic = f'{path_folder}{rinok}-int.xsd'
            self.path_label = f'{path_folder}{rinok}-int-label.xml'
            self.rinok = rinok
            self.rinok_folder = rinok_folder
        else:
            self.rinok_folder=rinok_folder
            self.rinok=rinok
            path_folder = f'{os.getcwd()}\{taxonomy}\\www.cbr.ru\\xbrl\\nso\\{rinok_folder}\\dic\\'
            self.path_dic=f'{path_folder}{rinok}-dic.xsd'
            self.path_label=f'{path_folder}{rinok}-dic-label.xml'
            self.path_definition=f'{path_folder}{rinok}-dic-definition.xml'

        self.df_roletype = pd.DataFrame(columns=['rinok','entity', 'id', 'roleuri', 'definition', 'uo_pres', 'uo_def', 'uo_gen'])
        self.df_element = pd.DataFrame(columns=['rinok','entity', 'targetnamespase', 'name', 'id', 'type', 'typeddomainref', 'substitutiongroup','periodtype', 'abstract', 'nillable', 'creationdate', 'fromdate'])
        self.df_locators = pd.DataFrame(columns=['rinok','entity','locfrom','parentrole', 'type', 'href', 'label', 'title'])
        self.df_labels = pd.DataFrame(columns=['rinok', 'entity','parentrole', 'type', 'label', 'role', 'title', 'lang', 'id', 'text'])
        self.df_arcs = pd.DataFrame(columns=['rinok','entity','arctype','parentrole', 'type', 'arcrole',
                                             'arcfrom', 'arcto', 'title','usable','order','use','priority','closed'])
        self.df_roleref=pd.DataFrame(columns=['rinok','entity','rolefrom','roleuri','type','href'])

    def writedef(self,defpres,*args):
        if args:
            path=args[0]
        else:
            path=self.path_definition.replace('definition', defpres)
        soup_roleref=self.parsetag(path,'link:linkbase')
        for xx in soup_roleref.find_all('link:roleref'):
            self.df_roleref.loc[-1]=[self.rinok,os.path.basename(path),defpres,
                                     xx['roleuri'] if 'roleuri' in xx.attrs.keys() else '',
                                     xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                                     xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else ''
                                     ]
            self.df_roleref.index = self.df_roleref.index + 1
            self.df_roleref = self.df_roleref.sort_index()

        soup_definition_loc=self.parsetag(path,'link:linkbase').find_all(f'link:{defpres}link')
        for xx in soup_definition_loc:
            for loc in xx.find_all('link:loc'):
                self.df_locators.loc[-1]=[self.rinok,os.path.basename(path),defpres,
                                        xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else '',
                                        loc['xlink:type'] if 'xlink:type' in loc.attrs.keys() else '',
                                        loc['xlink:href'] if 'xlink:href' in loc.attrs.keys() else '',
                                        loc['xlink:label'] if 'xlink:label' in loc.attrs.keys() else '',
                                        loc['xlink:title'] if 'xlink:title' in loc.attrs.keys() else '']
                self.df_locators.index = self.df_locators.index + 1
                self.df_locators = self.df_locators.sort_index()

            for arc in xx.find_all(f'link:{defpres}arc'):
                self.df_arcs.loc[-1] = [
                    self.rinok, os.path.basename(path),
                    f'link:{defpres}arc',
                    xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else '',
                    arc['xlink:type'] if 'xlink:type' in arc.attrs.keys() else '',
                    arc['xlink:arcrole'] if 'xlink:arcrole' in arc.attrs.keys() else '',
                    arc['xlink:from'] if 'xlink:from' in arc.attrs.keys() else '',
                    arc['xlink:to'] if 'xlink:to' in arc.attrs.keys() else '',
                    arc['xlink:title'] if 'xlink:title' in arc.attrs.keys() else '',
                    arc['xbrldt:usable'] if 'xbrldt:usable' in arc.attrs.keys() else '',
                    arc['order'] if 'order' in arc.attrs.keys() else '',
                    arc['use'] if 'use' in arc.attrs.keys() else '',
                    arc['priority'] if 'priority' in arc.attrs.keys() else '',
                    arc['closed'] if 'closed' in arc.attrs.keys() else ''
                ]
                self.df_arcs.index = self.df_arcs.index + 1
                self.df_arcs = self.df_arcs.sort_index()

        del soup_definition_loc,soup_roleref
        gc.collect()
        return ({'df_locators':self.df_locators,
                 'df_arcs':self.df_arcs,
                 'df_roleref':self.df_roleref})

    def writedic(self):
        soup_roletype=self.parsetag(self.path_dic,'xsd:appinfo')
        for xx in soup_roletype.find_all('link:roletype'):
            usedon = [yy.contents[0] for yy in xx.findAll('link:usedon')]
            self.df_roletype.loc[-1] = [self.rinok,os.path.basename(self.path_dic),
                                   xx['id'] if 'id' in xx.attrs.keys() else '',
                                   xx['roleuri'] if 'roleuri' in xx.attrs.keys() else '',
                                   xx.find_next('link:definition').contents[0],
                                   1 if 'link:presentationLink' in usedon else 0, \
                                   1 if 'link:definitionLink' in usedon else 0, \
                                   1 if 'gen:link' in usedon else 0]
            self.df_roletype.index = self.df_roletype.index + 1
            self.df_roletype = self.df_roletype.sort_index()

        tns=self.parseparam(self.path_dic,'xsd:schema','targetnamespace')
        soup_element=self.parsetag(self.path_dic,'xsd:schema')
        for xx in soup_element.find_all('xsd:element'):
            self.df_element.loc[-1] = [
                self.rinok,os.path.basename(self.path_dic),
                tns,
                xx['name'] if 'name' in xx.attrs else '',
                xx['id'] if 'id' in xx.attrs else '',
                xx['type'] if 'type' in xx.attrs else '',
                xx['xbrldt:typeddomainref'] if 'xbrldt:typeddomainref' in xx.attrs else '',
                xx['substitutiongroup'] if 'substitutiongroup' in xx.attrs else '',
                xx['xbrli:periodtype'] if 'xbrli:periodtype' in xx.attrs else '',
                xx['abstract'] if 'abstract' in xx.attrs else '',
                xx['nillable'] if 'nillable' in xx.attrs else '',
                xx['model:creationdate'] if 'model:creationdate' in xx.attrs else '',
                xx['model:fromdate'] if 'model:fromdate' in xx.attrs else '']
            self.df_element.index = self.df_element.index + 1
            self.df_element = self.df_element.sort_index()
        del soup_roletype,soup_element
        gc.collect()

    def parsetag(self,filepath,main_tree):
        with open(filepath,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml')
        soup_root=soup.contents[2]
        soup_tree=soup_root.find_next(main_tree)
        del soup,soup_root
        gc.collect()
        return soup_tree

    def parseparam(self,filepath,main_tree,tag):
        with open(filepath,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml')
        soup_root=soup.contents[2]
        soup_tree=soup_root.find_next(main_tree)
        soup_tag=soup_tree[tag]
        del soup,soup_root,soup_tree
        gc.collect()
        return soup_tag

    def writeloc(self):
        #print('запись локаторов запущена')
        soup_loc=self.parsetag(self.path_label, 'link:labellink')
        for xx in soup_loc.find_all('link:loc'):
            self.df_locators.loc[-1] = [
                self.rinok,os.path.basename(self.path_label),'label',
                soup_loc['xlink:role'] if 'xlink:role' in soup_loc.attrs.keys() else '',
                xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else '',
                xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else '',
                xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else '']

            self.df_locators.index = self.df_locators.index + 1
            self.df_locators = self.df_locators.sort_index()
        del soup_loc
        gc.collect()
        #print('запись локаторов завершена')

    def writelabel(self):
        #print('запись лейблов запущена')
        soup_roleref = self.parsetag(self.path_label, 'link:linkbase')
        for xx in soup_roleref.find_all('link:roleref'):
            self.df_roleref.loc[-1]=[self.rinok,os.path.basename(self.path_label),'link:linkbase',
                                     xx['roleuri'] if 'roleuri' in xx.attrs.keys() else '',
                                     xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                                     xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else ''
                                     ]
            self.df_roleref.index = self.df_roleref.index + 1
            self.df_roleref = self.df_roleref.sort_index()

        soup_label=self.parsetag(self.path_label, 'link:labellink')
        for xx in soup_label.find_all('link:label'):
            self.df_labels.loc[-1] = [
                self.rinok,os.path.basename(self.path_label),
                soup_label['xlink:role'] if 'xlink:role' in soup_label.attrs.keys() else '',
                xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else '',
                xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else '',
                xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else '',
                xx['xml:lang'] if 'xml:lang' in xx.attrs.keys() else '',
                xx['id'] if 'id' in xx.attrs.keys() else '',
                xx.text
            ]
            self.df_labels.index = self.df_labels.index + 1
            self.df_labels = self.df_labels.sort_index()
        del soup_label
        gc.collect()

    def writearc(self):
        #print('запись арок запущена')
        soup_arc=self.parsetag(self.path_label, 'link:labellink')
        for arc in soup_arc.find_all('link:labelarc'):
            self.df_arcs.loc[-1] = [
                self.rinok, os.path.basename(self.path_label),
                'link:labellink',
                soup_arc['xlink:role'] if 'xlink:role' in soup_arc.attrs.keys() else '',
                arc['xlink:type'] if 'xlink:type' in arc.attrs.keys() else '',
                arc['xlink:arcrole'] if 'xlink:arcrole' in arc.attrs.keys() else '',
                arc['xlink:from'] if 'xlink:from' in arc.attrs.keys() else '',
                arc['xlink:to'] if 'xlink:to' in arc.attrs.keys() else '',
                arc['xlink:title'] if 'xlink:title' in arc.attrs.keys() else '',
                arc['xbrldt:usable'] if 'xbrldt:usable' in arc.attrs.keys() else '',
                arc['order'] if 'order' in arc.attrs.keys() else '',
                arc['use'] if 'use' in arc.attrs.keys() else '',
                arc['priority'] if 'priority' in arc.attrs.keys() else '',
                arc['closed'] if 'closed' in arc.attrs.keys() else ''
            ]
            self.df_arcs.index = self.df_arcs.index + 1
            self.df_arcs =self.df_arcs.sort_index()
        #print('запись арок завершена')

    def writeThread(self,func):
        func()

    def returnDF_temp(self):
        funcs=[self.writearc,self.writeloc,self.writelabel,self.writedic]
        with ThreadPool(processes=7) as pool:
            pool.map(self.writeThread, funcs)
        return (
            {'df_rolerefs':self.df_roleref,
                'df_roletypes':self.df_roletype,
                'df_elements':self.df_element,
                'df_locators':self.df_locators,
                'df_arcs':self.df_arcs,
             'df_labels':self.df_labels}
                )

    def returnDF(self):
        self.returnDF_temp()
        self.writedef('definition')
        try: self.writedef('presentation')
        except: pass
        return (
            {'df_rolerefs':self.df_roleref,
                'df_roletypes':self.df_roletype,
                'df_elements':self.df_element,
                'df_locators':self.df_locators,
                'df_arcs':self.df_arcs,
             'df_labels':self.df_labels}
                )

if __name__ == "__main__":
    #ss=c_parseDic('final_5_2','udr\\dom','mem')
    ss=c_parseDic('final_5_2','uk','uk')
    print(ss.path_dic)
    print(ss.path_label)
    ss.returnDF_temp()
    None

