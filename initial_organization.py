#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  9 13:26:59 2019

@author: maxence.faldor
"""

from sqlalchemy import create_engine, MetaData, Table, select
import pandas as pd
import spacy
import wikipedia

# Spacy
nlp = spacy.load("en_core_web_lg")

def clean_organization(text):
    try:
        return nlp(text).ents[0].text
    except:
        return None

# Organization
organization = pd.DataFrame(columns=['post_id', 'name'])
index_organization = 0

for index, row in el_pprune_post.iterrows():
    for ent in nlp(row['content']).ents:
        if ent.label_ in ['ORG', 'GPE']:
            organization.loc[index_organization] = [row['post_id'], clean_organization(ent.text)]
            index_organization += 1

organization.to_excel(r'./organization-backup.xlsx', index=False, encoding='utf-8')

# Organization reduced
organization_reduced = organization.dropna(subset=['name'])
organization_reduced.drop_duplicates(subset=['name'], inplace=True)
organization_reduced = organization_reduced.where((pd.notnull(organization_reduced)), None)
organization_reduced = organization_reduced[organization_reduced.name.apply(len) > 2]
organization_reduced.reset_index(drop=True, inplace=True)
organization_reduced['link'] = None

# Link organization to wikipedia
def get_organization_link(organization_name):
    try:
        search = wikipedia.search(organization_name)[0]
        return wikipedia.page(search).url
    except:
        return None
    
organization_reduced['link'] = organization_reduced.name.progress_apply(get_organization_link)

organization_reduced.to_excel(r'./organization_reduced_wikipedia-backup.xlsx')
        
# Connecting PPRuNe organizations to elseco organizations
el_s_link_organization = pd.read_sql_table(table_name='el_s_link_organization', con=engine, coerce_float=False, schema=dbschema)
el_s_link_organization = el_s_link_organization[~el_s_link_organization.link.isna()]

organization_reduced = pd.merge(organization_reduced, el_s_link_organization, how='left', on='link')
organization_reduced = organization_reduced.where((pd.notnull(organization_reduced)), None)

organization = pd.merge(organization, organization_reduced[['name', 'organization_id']], how='left', on='name')
organization = organization.where((pd.notnull(organization)), None)

matched_organization = organization[~organization.organization_id.isna()]
el_pprune_post = el_pprune_post.join(matched_organization[['index_post', 'organization_id']].set_index('index_post'), how='left')
