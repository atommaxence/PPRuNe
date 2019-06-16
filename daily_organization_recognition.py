#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu May 30 15:16:26 2019

@author: maxence.faldor
"""

import pandas as pd
import spacy
import wikipedia

def daily_organization_recognition(post, el_s_link_organization):
    def clean_organization(text):
        try:
            return nlp(text).ents[0].text
        except:
            return None

    def get_organization_link(organization_name):
        try:
            search = wikipedia.search(organization_name)[0]
            return wikipedia.page(search).url
        except:
            return None
    
    nlp = spacy.load("en_core_web_lg")
    
    organization = pd.DataFrame(columns=['post_id', 'link'])
    index_organization = 0
    
    for index, row in post.iterrows():
        for ent in nlp(row['content']).ents:
            if ent.label_ in ['ORG', 'GPE']:
                organization.loc[index_organization] = [row['post_id'], get_organization_link(clean_organization(ent.text))]
                index_organization += 1
    
    organization = pd.merge(organization, el_s_link_organization, how='left', on='link')
    organization.drop(columns=['link'], inplace=True)
    organization = organization.where((pd.notnull(organization)), None)
    organization.dropna(subset=['organization_id'], inplace=True)
    organization['occurrences'] = organization.groupby(['post_id', 'organization_id'])['post_id'].transform('size')
    organization.drop_duplicates(inplace=True)
    
    return organization
