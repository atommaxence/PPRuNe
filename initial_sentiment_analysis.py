#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  9 16:55:40 2019

@author: maxence.faldor
"""

import pandas as pd
from textblob import TextBlob
import re
from sqlalchemy import create_engine, MetaData, select, Table, update

def clean_sentiment(text):
    return re.sub(r'(\[QUOTE=.*?\]|\n|\r|\.{2,}|-{2,})', ' ', text)

def get_polarity(text):
    return TextBlob(text).sentiment.polarity

def get_subjectivity(text):
    return TextBlob(text).sentiment.subjectivity

post['polarity'] = post.apply(lambda row: get_polarity(clean_sentiment(row['content'])), axis=1)
post['subjectivity'] = post.apply(lambda row: get_subjectivity(clean_sentiment(row['content'])), axis=1)

for index, row in post.iterrows():
    connection.execute(update(el_pprune_post).values({'polarity': row['polarity'], 'subjectivity': row['subjectivity']}).where(el_pprune_post.columns.post_id == row['post_id']))

