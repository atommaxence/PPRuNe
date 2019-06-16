#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  9 11:48:03 2019

@author: maxence.faldor
"""

from sqlalchemy import create_engine, MetaData, select, Table, update, and_, or_
import pandas as pd
import spacy
import string
import re

forum_id = 0

# Connection to database
dbip = '192.168.14.21'
dbport = '32053'
dbusername = 'MAXENCEF'
dbpassword = 'Max123456'
dbschema = 'EL_RISK_DB'

engine = create_engine("hana+pyhdb://{}:{}@{}:{}".format(dbusername, dbpassword, dbip, dbport))
metadata = MetaData(engine)
connection = engine.connect()

# Calling the tables
el_pprune_thread = Table('el_pprune_thread', metadata,  schema=dbschema, autoload=True)
el_pprune_post = Table('el_pprune_post', metadata,  schema=dbschema, autoload=True)

# Threads
query_thread = select([el_pprune_thread]).where(el_pprune_thread.columns.forum_id == forum_id)

result_thread = connection.execute(query_thread).fetchall()
thread_id_list = [r[0] for r in result_thread]

# Transforming the table into a dataframe
thread = pd.DataFrame(result_thread)
thread.columns = ['thread_id', 'name', 'url', 'forum_id', 'created_by', 'created_date', 'modified_by', 'modified_date', 'topic']

# Posts
query_post = select([el_pprune_post]).where(and_(el_pprune_post.columns.thread_id.in_(thread_id_list), or_(el_pprune_post.columns.polarity.is_(None), el_pprune_post.columns.subjectivity.is_(None))))

result_post = connection.execute(query_post).fetchall()

# Transforming the table into a dataframe
post = pd.DataFrame(result_post)
post.columns = ['post_id', 'thread_id', 'user', 'date', 'content', 'polarity', 'subjectivity', 'created_by', 'created_date', 'modified_by', 'modified_date']

# Spacy
nlp = spacy.load("en_core_web_lg")
nlp.Defaults.stop_words |= {"like", "maybe", "make", "know", "think", "go", "look", "say", "post", "see", "get", "thing"}

# Cleaning content
post.dropna(subset=['content'], inplace=True)
punctuations = string.punctuation

def clean(text, keep_url=False):
    doc = nlp(text)
    tokens = []
    for token in doc:
        if not token.is_stop and token.text.strip() not in punctuations and token.pos_ != 'NUM':
            if token.like_url:
                if keep_url:
                    tokens.append(token.text.strip())
            else:
                tokens.append(token.lemma_.strip())
    return ' '.join(tokens)

post['clean_content'] = post.content.apply(lambda text: text.encode('ascii').decode('utf8'))
post['clean_content'] = post.clean_content.apply(lambda text: re.sub(r'(\[QUOTE=.*?\]|\n|\r|\.{2,}|-{2,})', ' ', text))
post['clean_content'] = post.clean_content.apply(clean, keep_url=False)
post['clean_content'] = post.clean_content.apply(lambda text: text.encode('ascii').decode('utf8'))

post.to_excel(r'./clean_forum_0.xlsx', index=False)
