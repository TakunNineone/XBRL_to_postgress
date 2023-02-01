import gc

import  pandas as pd
import os
from bs4 import BeautifulSoup
from multiprocessing.pool import ThreadPool

class c_parseToDf():
    def __init__(self,rinok):
        self.rinok=rinok
        self.df_rulenodes_Dic = []
        self.df_rulenodes_e_Dic=[]
        self.df_rulenodes_c_Dic = []
        self.df_rulenodes_p_Dic = []
        self.df_elements_Dic=[]
        self.df_roletypes_Dic=[]
        self.df_locators_Dic=[]
        self.df_arcs_Dic=[]
        self.df_labels_Dic=[]
        self.df_rolerefs_Dic=[]
        self.df_tableschemas_Dic=[]
        self.df_linkbaserefs_Dic=[]

        self.df_tables = pd.DataFrame(columns=['rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace'])

    def parseRulenodes(self,path):
        soup=self.parsetag(path,'linkbase').find_all_next('table:rulenode')
        df_rulenodes = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'abstract', 'merge', 'nexttag'])
        df_rulenodes_e = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id', 'dimension', 'member'])
        df_rulenodes_c = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id','value'])
        df_rulenodes_p = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id','period_type','start','end'])
        if soup:
            for xx in soup:
                parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None
                nexttag = xx.findChild()
                if nexttag:
                    df_rulenodes.loc[-1] = [self.rinok, os.path.basename(path),
                                            parentrole,
                                            xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                            xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                            xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                            xx['id'] if 'id' in xx.attrs.keys() else None,
                                            xx['abstract'] if 'abstract' in xx.attrs.keys() else None,
                                            xx['merge'] if 'merge' in xx.attrs.keys() else None,
                                            nexttag.name.replace('formula:','')
                                            ]
                    df_rulenodes.index = df_rulenodes.index + 1
                    df_rulenodes = df_rulenodes.sort_index()
                    if nexttag.name == 'formula:explicitdimension':
                        df_rulenodes_e.loc[-1] = [self.rinok, os.path.basename(path),
                                                         parentrole,
                                                         xx['id'] if 'id' in xx.attrs.keys() else None,
                                                         nexttag['dimension'] if 'dimension' in nexttag.attrs.keys() else None,
                                                         nexttag.text.strip()
                                                         ]
                        df_rulenodes_e.index = df_rulenodes_e.index + 1
                        df_rulenodes_e = df_rulenodes_e.sort_index()
                    elif nexttag.name == 'formula:concept':
                        df_rulenodes_c.loc[-1] = [self.rinok, os.path.basename(path),
                                                       parentrole,
                                                       xx['id'] if 'id' in xx.attrs.keys() else None,
                                                       nexttag.text.strip()
                                                       ]
                        df_rulenodes_c.index = df_rulenodes_c.index + 1
                        df_rulenodes_c = df_rulenodes_c.sort_index()
                    elif nexttag.name == 'formula:period':
                        df_rulenodes_p.loc[-1] = [self.rinok, os.path.basename(path),
                                                       parentrole,
                                                       xx['id'] if 'id' in xx.attrs.keys() else None,
                                                       nexttag.find_next().name.replace('formula:',''),
                                                       nexttag.find_next()['start'] if 'start' in nexttag.find_next().attrs.keys() else nexttag.find_next()['value'] if 'value' in nexttag.find_next().attrs.keys() else None,
                                                       nexttag.find_next()['end'] if 'end' in nexttag.find_next().attrs.keys() else nexttag.find_next()['value'] if 'value' in nexttag.find_next().attrs.keys() else None
                                                       ]
                        df_rulenodes_p.index = df_rulenodes_p.index + 1
                        df_rulenodes_p = df_rulenodes_p.sort_index()
        self.appendDfs_Dic(self.df_rulenodes_Dic,df_rulenodes)
        self.appendDfs_Dic(self.df_rulenodes_c_Dic, df_rulenodes_c)
        self.appendDfs_Dic(self.df_rulenodes_p_Dic, df_rulenodes_p)
        self.appendDfs_Dic(self.df_rulenodes_e_Dic, df_rulenodes_e)
        del df_rulenodes_e,df_rulenodes_p,df_rulenodes_c,df_rulenodes

    def parseElements(self,dict_with_lbrfs, full_file_path):
        #print(f'Elements - {full_file_path}')
        df_elements = pd.DataFrame(columns=['rinok', 'entity', 'targetnamespase', 'name', 'id', 'type',
                                                 'typeddomainref', 'substitutiongroup', 'periodtype', 'abstract',
                                                 'nillable', 'creationdate', 'fromdate', 'enumdomain', 'enum2domain',
                                                 'enumlinkrole', 'enum2linkrole'])
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                df_elements.loc[-1] = [
                    self.rinok,os.path.basename(full_file_path),
                    xx.parent['targetnamespace'] if 'targetnamespace' in xx.parent.attrs.keys() else None,
                    xx['name'] if 'name' in xx.attrs else None,
                    xx['id'] if 'id' in xx.attrs else None,
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
                    xx['enum2:linkrole'] if 'enum2:linkrole' in xx.attrs else None
                ]
                df_elements.index = df_elements.index + 1
                df_elements = df_elements.sort_index()
        self.appendDfs_Dic(self.df_elements_Dic, df_elements)
        del df_elements

    def parseLinkbaserefs(self, dict_with_lbrfs, full_file_path, tag):
        #print(f'Linkbaserefs - {full_file_path}')
        df_linkbaserefs = pd.DataFrame(columns=['rinok', 'entity', 'rolefrom', 'targetnamespace', 'type', 'href'])
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                df_linkbaserefs.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                           xx.parent.parent.parent['targetnamespace'] if 'targetnamespace' in xx.parent.parent.parent.attrs.keys() else None,
                                               xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                               xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None
                                           ]
                df_linkbaserefs.index = df_linkbaserefs.index + 1
                df_linkbaserefs = df_linkbaserefs.sort_index()
        self.appendDfs_Dic(self.df_linkbaserefs_Dic, df_linkbaserefs)
        del df_linkbaserefs

    def parseRoletypes(self,dict_with_rltps,full_file_path):
        #print(f'Roletypes - {full_file_path}')
        df_roletypes = pd.DataFrame(columns=['rinok', 'entity', 'id', 'roleuri', 'definition', 'uo_pres','uo_def', 'uo_gen'])
        if dict_with_rltps:
            for xx in dict_with_rltps:
                usedon = [yy.contents[0] for yy in xx.findAll('link:usedon')]
                df_roletypes.loc[-1] = [self.rinok, os.path.basename(full_file_path),
                                            xx['id'] if 'id' in xx.attrs.keys() else None,
                                            xx['roleuri'] if 'roleuri' in xx.attrs.keys() else None,
                                            xx.find_next('link:definition').contents[0] if xx.find_next('link:definition') else None,
                                            1 if 'link:presentationlink' in usedon else 0, \
                                            1 if 'link:definitionlink' in usedon else 0, \
                                            1 if 'gen:link' in usedon else 0]
                df_roletypes.index = df_roletypes.index + 1
                df_roletypes = df_roletypes.sort_index()
        self.appendDfs_Dic(self.df_roletypes_Dic, df_roletypes)
        del df_roletypes

    def parseLabels(self,dict_with_lbls,full_file_path):
        #print(f'Labels - {full_file_path}')
        df_labels = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'role', 'title',
                                               'lang', 'id', 'text'])
        if dict_with_lbls:
            for xx in dict_with_lbls:
                df_labels.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path),
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                    xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else None,
                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                    xx['xml:lang'] if 'xml:lang' in xx.attrs.keys() else None,
                    xx['id'] if 'id' in xx.attrs.keys() else None,
                    xx.text
                ]
                df_labels.index = df_labels.index + 1
                df_labels = df_labels.sort_index()
        self.appendDfs_Dic(self.df_labels_Dic, df_labels)
        del df_labels

    def parseLocators(self,dict_with_loc,full_file_path,tag):
        df_locators = pd.DataFrame(columns=['rinok', 'entity', 'locfrom', 'parentrole', 'type', 'href','href_id', 'label', 'title'])
        #print(f'Locators - {full_file_path}')
        if dict_with_loc:
            for xx in dict_with_loc:
                df_locators.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path), tag,
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                    xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None,
                    xx['xlink:href'].split('#')[1] if 'xlink:href' in xx.attrs.keys() else None,
                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None]
                df_locators.index = df_locators.index + 1
                df_locators = df_locators.sort_index()
        self.appendDfs_Dic(self.df_locators_Dic, df_locators)
        del df_locators

    def parseRolerefs(self,dict_with_rlrfs,full_file_path,tag):
        #print(f'Rolerefs - {full_file_path}')
        df_rolerefs = pd.DataFrame(columns=['rinok', 'entity', 'rolefrom', 'roleuri', 'type', 'href'])
        if dict_with_rlrfs:
            for xx in dict_with_rlrfs:
                df_rolerefs.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                           xx['roleuri'] if 'roleuri' in xx.attrs.keys() else None,
                                           xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                           xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else None
                                           ]
                df_rolerefs.index = df_rolerefs.index + 1
                df_rolerefs = df_rolerefs.sort_index()
        self.appendDfs_Dic(self.df_rolerefs_Dic, df_rolerefs)
        del df_rolerefs

    def parseTableschemas(self,dict_with_tsch,full_file_path,tag):
        #print(f'Tableschemas - {full_file_path}')
        df_tableschemas = pd.DataFrame(
            columns=['rinok', 'entity', 'rolefrom', 'parentrole', 'type', 'label', 'title', 'id', 'parentchildorder'])
        if dict_with_tsch:
            for xx in dict_with_tsch:
                df_tableschemas.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                                xx.parent['xlink:type'] if 'xlink:type' in xx.parent.attrs.keys() else None,
                                                xx['xlink:role'] if 'xlink:role' in xx.attrs.keys() else None,
                                                xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                                xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                                xx['id'] if 'id' in xx.attrs.keys() else None,
                                                xx['parentchildorder'] if 'parentchildorder' in xx.attrs.keys() else None
                                           ]
                df_tableschemas.index = df_tableschemas.index + 1
                df_tableschemas = df_tableschemas.sort_index()
        self.appendDfs_Dic(self.df_tableschemas_Dic, df_tableschemas)
        del df_tableschemas

    def parseArcs(self,dict_with_arcs,full_file_path,tag):
        #print(f'Arcs - {full_file_path}')
        df_arcs = pd.DataFrame(columns=['rinok', 'entity', 'arctype', 'parentrole', 'type', 'arcrole',
                                             'arcfrom', 'arcto', 'title', 'usable', 'closed', 'contextelement',
                                             'targetrole', 'order', 'preferredlabel', 'use', 'priority'
                                             ])
        if dict_with_arcs:
            for arc in dict_with_arcs:
                df_arcs.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path),
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
                    arc['priority'] if 'priority' in arc.attrs.keys() else None
                ]
                df_arcs.index = df_arcs.index + 1
                df_arcs = df_arcs.sort_index()
        self.appendDfs_Dic(self.df_arcs_Dic, df_arcs)
        del df_arcs

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
        soup_tree=soup_root.find_next(main_tree)
        return soup_tree

