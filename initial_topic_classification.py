#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  9 14:43:57 2019

@author: maxence.faldor
"""

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import NMF

# NMF model
thread['content'] = None
for index, row in thread.iterrows():
    thread.at[index, 'content'] = ' '.join([content for content in post[post.thread_id == row['thread_id']]['clean_content']])

number_of_topics = 10

vectorizer_NMF = TfidfVectorizer()
transformed_NMF = vectorizer_NMF.fit_transform(thread.content.to_list())
feature_name_NMF = vectorizer_NMF.get_feature_names()

model = NMF(n_components=number_of_topics)
model.fit(transformed_NMF)

W_NMF = model.transform(transformed_NMF)
H_NMF = model.components_

# Constructing the frame of topics and its dictionnary
def get_nmf_topics(model, number_of_words):
    feature_names = vectorizer_NMF.get_feature_names()
    word_dict = {}
    topic_number = 0
    for i in range(number_of_topics):
        if i in [0, 9]:
            continue
        words_ids = model.components_[i].argsort()[:-number_of_words-1:-1]
        words = [feature_names[key] for key in words_ids]
        word_dict['{:02d}'.format(topic_number)] = words
        topic_number += 1

    return pd.DataFrame(word_dict)

topic = get_nmf_topics(model, 25)

topics = {}
for j in range(topic.shape[1]):
    topics[j] = topic.iloc[:, j].to_list()

# Classify each thread into a topic
thread['topic'] = None
for index, row in thread.iterrows():
    thread_words = row['content'].split()
    counter = [0]*len(topics)
    
    for topic_index, topic_words in enumerate(topics.values()):
        for topic_word in topic_words:
            counter[topic_index] += thread_words.count(topic_word)
    
    if len([i for i, v in enumerate(counter) if v == max(counter)]) == 1:
        thread.at[index, 'topic'] = np.argmax(counter)
    
    else:
        thread.at[index, 'topic'] = None
        
