import  pandas as pd
import os
from bs4 import BeautifulSoup

class c_parseToDf():
    def __init__(self,rinok):
        self.rinok=rinok
        self.df_elements = pd.DataFrame(
            columns=['rinok','entity', 'targetnamespase', 'name', 'id', 'type', 'typeddomainref', 'substitutiongroup',
                     'periodtype', 'abstract', 'nillable', 'creationdate', 'fromdate','enumdomain','enum2domain','enumlinkrole','enum2linkrole'])
        self.df_tables = pd.DataFrame(columns=['rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace'])
        self.df_roletypes = pd.DataFrame(
            columns=['rinok', 'entity', 'id', 'roleuri', 'definition', 'uo_pres', 'uo_def', 'uo_gen'])
        self.df_locators = pd.DataFrame(
            columns=['rinok', 'entity', 'locfrom', 'parentrole', 'type', 'href', 'label', 'title'])
        self.df_labels = pd.DataFrame(
            columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'role', 'title', 'lang', 'id', 'text'])
        self.df_arcs = pd.DataFrame(columns=['rinok', 'entity', 'arctype', 'parentrole', 'type', 'arcrole',
                                             'arcfrom', 'arcto', 'title', 'usable', 'order', 'use', 'priority',
                                             'closed'])
        self.df_rolerefs = pd.DataFrame(columns=['rinok', 'entity', 'rolefrom', 'roleuri', 'type', 'href'])
        self.df_linkbaserefs = pd.DataFrame(columns=['rinok', 'entity', 'rolefrom', 'targetnamespace', 'type', 'href'])
        self.df_tableschemas = pd.DataFrame(
            columns=['rinok', 'entity', 'rolefrom', 'parentrole', 'type', 'label', 'title', 'id', 'parentChildOrder'])
        self.df_rulenodes = pd.DataFrame(
            columns=['rinok', 'entity', 'rolefrom', 'parentrole', 'type', 'label', 'title', 'id', 'abstract',
                     'merge', ])
    def parseElements(self,dict_with_lbrfs, full_file_path):
        if dict_with_lbrfs:
            print(f'Elements - {full_file_path}')
            for xx in dict_with_lbrfs:
                self.df_elements.loc[-1] = [
                    self.rinok,os.path.basename(full_file_path),
                    xx.parent['targetnamespace'] if 'targetnamespace' in xx.parent.attrs.keys() else '',
                    xx['name'] if 'name' in xx.attrs else '',
                    xx['id'] if 'id' in xx.attrs else '',
                    xx['type'] if 'type' in xx.attrs else '',
                    xx['xbrldt:typeddomainref'] if 'xbrldt:typeddomainref' in xx.attrs else '',
                    xx['substitutiongroup'] if 'substitutiongroup' in xx.attrs else '',
                    xx['xbrli:periodtype'] if 'xbrli:periodtype' in xx.attrs else '',
                    xx['abstract'] if 'abstract' in xx.attrs else '',
                    xx['nillable'] if 'nillable' in xx.attrs else '',
                    xx['model:creationdate'] if 'model:creationdate' in xx.attrs else '',
                    xx['model:fromdate'] if 'model:fromdate' in xx.attrs else '',
                    xx['enum:domain'] if 'enum:domain' in xx.attrs else '',
                    xx['enum2:domain'] if 'enum2:domain' in xx.attrs else '',
                    xx['enum:linkrole'] if 'enum:linkrole' in xx.attrs else '',
                    xx['enum2:linkrole'] if 'enum2:linkrole' in xx.attrs else ''
                ]
                self.df_elements.index = self.df_elements.index + 1
                self.df_elements = self.df_elements.sort_index()

    def parseLinkbaserefs(self, dict_with_lbrfs, full_file_path, tag):
        print(f'Linkbaserefs - {full_file_path}')
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                self.df_linkbaserefs.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                           xx.parent.parent.parent['targetnamespace'] if 'targetnamespace' in xx.parent.parent.parent.attrs.keys() else '',
                                               xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                                               xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else ''
                                           ]
                self.df_linkbaserefs.index = self.df_linkbaserefs.index + 1
                self.df_linkbaserefs = self.df_linkbaserefs.sort_index()

    def parseRoletypes(self,dict_with_rltps,full_file_path):
        print(f'Roletypes - {full_file_path}')
        if dict_with_rltps:
            for xx in dict_with_rltps:
                usedon = [yy.contents[0] for yy in xx.findAll('link:usedon')]
                self.df_roletypes.loc[-1] = [self.rinok, os.path.basename(full_file_path),
                                            xx['id'] if 'id' in xx.attrs.keys() else '',
                                            xx['roleuri'] if 'roleuri' in xx.attrs.keys() else '',
                                            xx.find_next('link:definition').contents[0] if xx.find_next('link:definition') else '',
                                            1 if 'link:presentationLink' in usedon else 0, \
                                            1 if 'link:definitionLink' in usedon else 0, \
                                            1 if 'gen:link' in usedon else 0]
                self.df_roletypes.index = self.df_roletypes.index + 1
                self.df_roletypes = self.df_roletypes.sort_index()

    def parseLabels(self,dict_with_lbls,full_file_path):
        print(f'Labels - {full_file_path}')
        if dict_with_lbls:
            for xx in dict_with_lbls:
                self.df_labels.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path),
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else '',
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

    def parseLocators(self,dict_with_loc,full_file_path,tag):
        print(f'Locators - {full_file_path}')
        if dict_with_loc:
            for xx in dict_with_loc:
                self.df_locators.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path), tag,
                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else '',
                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                    xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else '',
                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else '',
                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else '']
                self.df_locators.index = self.df_locators.index + 1
                self.df_locators = self.df_locators.sort_index()

    def parseRolerefs(self,dict_with_rlrfs,full_file_path,tag):
        print(f'Rolerefs - {full_file_path}')
        if dict_with_rlrfs:
            for xx in dict_with_rlrfs:
                self.df_rolerefs.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                           xx['roleuri'] if 'roleuri' in xx.attrs.keys() else '',
                                           xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                                           xx['xlink:href'] if 'xlink:href' in xx.attrs.keys() else ''
                                           ]
                self.df_rolerefs.index = self.df_rolerefs.index + 1
                self.df_rolerefs = self.df_rolerefs.sort_index()

    def parseTableschemas(self,dict_with_tsch,full_file_path,tag):
        print(f'Tableschemas - {full_file_path}')
        if dict_with_tsch:
            for xx in dict_with_tsch:
                self.df_tableschemas.loc[-1] = [self.rinok, os.path.basename(full_file_path), tag,
                                                xx.parent['xlink:type'] if 'xlink:type' in xx.parent.attrs.keys() else '',
                                                xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else '',
                                                xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else '',
                                                xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else '',
                                                xx['id'] if 'id' in xx.attrs.keys() else '',
                                                xx['parentchildorder'] if 'parentchildorder' in xx.attrs.keys() else ''
                                           ]
                self.df_tableschemas.index = self.df_tableschemas.index + 1
                self.df_tableschemas = self.df_tableschemas.sort_index()

    def parseArcs(self,dict_with_arcs,full_file_path,tag):
        print(f'Arcs - {full_file_path}')
        if dict_with_arcs:
            for arc in dict_with_arcs:
                self.df_arcs.loc[-1] = [
                    self.rinok, os.path.basename(full_file_path),
                    tag,
                    arc.parent['xlink:role'] if 'xlink:role' in arc.parent.attrs.keys() else '',
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

    def parsetag(self,filepath,main_tree):
        with open(filepath,'rb') as f:
            ff=f.read()
        soup=BeautifulSoup(ff,'lxml')
        soup_root=soup.contents[2]
        soup_tree=soup_root.find_next(main_tree)
        return soup_tree

    def returtDfs(self):
        return (
            {
                'df_tables':self.df_tables,
                'df_roletypes':self.df_roletypes,
                'df_labels':self.df_labels,
                'df_arcs':self.df_arcs,
                'df_rolerefs':self.df_rolerefs,
                'df_linkbaserefs':self.df_linkbaserefs,
                'df_rulenodes':self.df_rulenodes,
                'df_tableschemas':self.df_tableschemas,
                'df_elements':self.df_elements,
                'df_locators':self.df_locators
            }
        )