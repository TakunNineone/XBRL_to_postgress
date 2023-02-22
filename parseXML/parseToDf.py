import gc
import re

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
        self.df_va_edmembers_Dic = []
        self.df_va_edimensions_Dic = []
        self.df_va_tdimensions_Dic = []
        self.df_va_concepts_Dic = []
        self.df_va_factvars_Dic = []
        self.df_va_assertions_Dic = []
        self.df_va_generals_Dic=[]

        self.df_tables = pd.DataFrame(columns=['rinok', 'entity', 'targetnamespace', 'schemalocation', 'namespace'])

    def parse_assertions(self,soup,path):
        df_va_assertions = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'test','variables', 'aspectmodel','implicitfiltering'])
        soup=soup.find_all_next('va:valueassertion')
        for xx in soup:
            str=None
            if 'test' in xx.attrs.keys():
                str=''
                variables=re.findall('[^\w\s]\S*',xx['test'])
            #     variables=re.findall('\S*', xx['test'])
            #     for vv in variables:
            #         if vv:
            #             if vv[0]=='$':
            #                 str=str+re.sub(r"[^\w]*$", '', vv[1:])+'|'
            # str=str[:-1]
            df_va_assertions.loc[-1] = [self.rinok, os.path.basename(path),
                                    xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                    xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                    xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                    xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                    xx['id'] if 'id' in xx.attrs.keys() else None,
                                    xx['test'] if 'test' in xx.attrs.keys() else None,
                                    '',
                                    xx['aspectmodel'] if 'aspectmodel' in xx.attrs.keys() else None,
                                    xx['implicitfiltering'] if 'implicitfiltering' in xx.attrs.keys() else None
                                    ]
            df_va_assertions.index = df_va_assertions.index + 1
            df_va_assertions = df_va_assertions.sort_index()
        self.df_va_assertions_Dic.append(df_va_assertions)

    def parse_generals(self,soup,path):
        df_va_generals=pd.DataFrame(columns=['rinok', 'entity', 'parentrole','label','title','id','test'])
        for xx in soup:
            df_va_generals.loc[-1] = [self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['test'] if 'test' in xx.attrs.keys() else None,
                                        ]
            df_va_generals.index = df_va_generals.index + 1
            df_va_generals = df_va_generals.sort_index()
        self.df_va_generals_Dic.append(df_va_generals)

    def parse_factvars(self,soup,path):
        df_va_factvars = pd.DataFrame( columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'bindassequence','fallbackvalue'])
        soup=soup.find_all_next('variable:factvariable')
        for xx in soup:
            df_va_factvars.loc[-1] = [self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['bindassequence'] if 'bindassequence' in xx.attrs.keys() else None,
                                        xx['fallbackvalue'] if 'fallbackvalue' in xx.attrs.keys() else None
                                        ]
            df_va_factvars.index = df_va_factvars.index + 1
            df_va_factvars = df_va_factvars.sort_index()
        self.df_va_factvars_Dic.append(df_va_factvars)

    def parse_concepts(self,soup,path):
        df_va_concepts = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'value'])
        soup=soup.find_all_next('cf:conceptname')
        for xx in soup:
            df_va_concepts.loc[-1] = [self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx.text.strip()
                                        ]
            df_va_concepts.index = df_va_concepts.index + 1
            df_va_concepts = df_va_concepts.sort_index()
        self.df_va_concepts_Dic.append(df_va_concepts)

    def parse_tdimensions(self,soup,path):
        df_va_tdimensions = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'value'])
        soup=soup.find_all_next('df:typeddimension')
        for xx in soup:
            df_va_tdimensions.loc[-1] = [self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx.text.strip()
                                        ]
            df_va_tdimensions.index = df_va_tdimensions.index + 1
            df_va_tdimensions = df_va_tdimensions.sort_index()
        self.df_va_tdimensions_Dic.append(df_va_tdimensions)

    def parse_edimensions(self,soup,path):
        df_va_edimensions = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'dimension'])
        df_va_edmembers = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'dimension_id', 'member'])
        soup=soup.find_all_next('df:explicitdimension')
        for xx in soup:
            df_va_edimensions.loc[-1] = [self.rinok, os.path.basename(path),
                                        xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx.findChild().text.strip()
                                        ]
            df_va_edimensions.index = df_va_edimensions.index + 1
            df_va_edimensions = df_va_edimensions.sort_index()
            members=xx.find_all('df:member')
            if members!=[]:
                for yy in members:
                    df_va_edmembers.loc[-1] = [self.rinok, os.path.basename(path),
                                                 xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                                 xx['id'] if 'id' in xx.attrs.keys() else None,
                                                 yy.text.strip()
                                                 ]
                    df_va_edmembers.index = df_va_edmembers.index + 1
                    df_va_edmembers = df_va_edmembers.sort_index()
        self.df_va_edmembers_Dic.append(df_va_edmembers)
        self.df_va_edimensions_Dic.append(df_va_edimensions)

    def parse_covers(self,soup,path):
        df_va_covers = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'values'])
        soup = soup.find_all_next('df:typeddimension')
        for xx in soup:
            df_va_tdimensions.loc[-1] = [self.rinok, os.path.basename(path),
                                         xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
                                         xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                         xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                         xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                         xx['id'] if 'id' in xx.attrs.keys() else None,
                                         xx.text.strip()
                                         ]
            df_va_tdimensions.index = df_va_tdimensions.index + 1
            df_va_tdimensions = df_va_tdimensions.sort_index()
        self.df_va_tdimensions_Dic.append(df_va_tdimensions)

    # def parseVA(self,path):
    #     df_va_covers = pd.DataFrame(columns=['rinok', 'entity', 'parentrole','type', 'label', 'title', 'id','values'])


    def parseRulenodes(self,path):
        # try:
        if self.parsetag(path,'linkbase'):
            soup=self.parsetag(path,'linkbase').find_all_next('table:rulenode')
        else:
            soup = self.parsetag(path,'link:linkbase').find_all_next('table:rulenode')
        df_rulenodes = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'type', 'label', 'title', 'id', 'abstract', 'merge'])
        df_rulenodes_e = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id', 'dimension', 'member'])
        df_rulenodes_c = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id','value'])
        df_rulenodes_p = pd.DataFrame(columns=['rinok', 'entity', 'parentrole', 'rulenode_id','period_type','start','end'])
        if soup:
            for xx in soup:
                parentrole = xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None
                nexttag_e = xx.find_next('formula:explicitdimension')
                nexttag_p = xx.find_next('formula:period')
                nexttag_c = xx.find_next('formula:concept')
                df_rulenodes.loc[-1] = [self.rinok, os.path.basename(path),
                                        parentrole,
                                        xx['xlink:type'] if 'xlink:type' in xx.attrs.keys() else None,
                                        xx['xlink:label'] if 'xlink:label' in xx.attrs.keys() else None,
                                        xx['xlink:title'] if 'xlink:title' in xx.attrs.keys() else None,
                                        xx['id'] if 'id' in xx.attrs.keys() else None,
                                        xx['abstract'] if 'abstract' in xx.attrs.keys() else None,
                                        xx['merge'] if 'merge' in xx.attrs.keys() else None
                                        ]
                df_rulenodes.index = df_rulenodes.index + 1
                df_rulenodes = df_rulenodes.sort_index()
                if nexttag_e:
                    df_rulenodes_e.loc[-1] = [self.rinok, os.path.basename(path),
                                                     parentrole,
                                                     xx['id'] if 'id' in xx.attrs.keys() else None,
                                                     nexttag_e['dimension'] if 'dimension' in nexttag_e.attrs.keys() else None,
                                                     nexttag_e.text.strip()
                                                     ]
                    df_rulenodes_e.index = df_rulenodes_e.index + 1
                    df_rulenodes_e = df_rulenodes_e.sort_index()
                if nexttag_c:
                    df_rulenodes_c.loc[-1] = [self.rinok, os.path.basename(path),
                                                   parentrole,
                                                   xx['id'] if 'id' in xx.attrs.keys() else None,
                                                   nexttag_c.text.strip()
                                                   ]
                    df_rulenodes_c.index = df_rulenodes_c.index + 1
                    df_rulenodes_c = df_rulenodes_c.sort_index()
                if nexttag_p:
                    df_rulenodes_p.loc[-1] = [self.rinok, os.path.basename(path),
                                                   parentrole,
                                                   xx['id'] if 'id' in xx.attrs.keys() else None,
                                                   nexttag_p.find_next().name.replace('formula:',''),
                                                   nexttag_p.find_next()['start'] if 'start' in nexttag_p.find_next().attrs.keys() else nexttag_p.find_next()['value'] if 'value' in nexttag_p.find_next().attrs.keys() else None,
                                                   nexttag_p.find_next()['end'] if 'end' in nexttag_p.find_next().attrs.keys() else nexttag_p.find_next()['value'] if 'value' in nexttag_p.find_next().attrs.keys() else None
                                                   ]
                    df_rulenodes_p.index = df_rulenodes_p.index + 1
                    df_rulenodes_p = df_rulenodes_p.sort_index()
        self.appendDfs_Dic(self.df_rulenodes_Dic,df_rulenodes)
        self.appendDfs_Dic(self.df_rulenodes_c_Dic, df_rulenodes_c)
        self.appendDfs_Dic(self.df_rulenodes_p_Dic, df_rulenodes_p)
        self.appendDfs_Dic(self.df_rulenodes_e_Dic, df_rulenodes_e)
        del df_rulenodes_e,df_rulenodes_p,df_rulenodes_c,df_rulenodes
        # except Exception as err:
        #     print(path)
        #     print(f"Unexpected {err=}, {type(err)=}")

    def parseElements(self,dict_with_lbrfs, full_file_path):
        #print(f'Elements - {full_file_path}')
        df_elements = pd.DataFrame(columns=['rinok', 'entity', 'targetnamespase', 'name', 'id','qname', 'type',
                                                 'typeddomainref', 'substitutiongroup', 'periodtype', 'abstract',
                                                 'nillable', 'creationdate', 'fromdate', 'enumdomain', 'enum2domain',
                                                 'enumlinkrole', 'enum2linkrole'])
        if dict_with_lbrfs:
            for xx in dict_with_lbrfs:
                qname_rep=os.path.basename(full_file_path).replace('.xsd','')
                df_elements.loc[-1] = [
                    self.rinok,os.path.basename(full_file_path),
                    xx.parent['targetnamespace'] if 'targetnamespace' in xx.parent.attrs.keys() else None,
                    xx['name'] if 'name' in xx.attrs else None,
                    xx['id'] if 'id' in xx.attrs else None,
                    xx['id'].replace(qname_rep+'_',qname_rep+':') if 'id' in xx.attrs else None,
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
                                                xx.parent['xlink:role'] if 'xlink:role' in xx.parent.attrs.keys() else None,
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
                                             'targetrole', 'order', 'preferredlabel', 'use', 'priority','complement','cover','name'
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
                    arc['priority'] if 'priority' in arc.attrs.keys() else None,
                    arc['complement'] if 'complement' in arc.attrs.keys() else None,
                    arc['cover'] if 'cover' in arc.attrs.keys() else None,
                    arc['name'] if 'name' in arc.attrs.keys() else None
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

