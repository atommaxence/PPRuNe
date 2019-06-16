#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 15:27:40 2019

@author: maxence.faldor
"""

import numpy as np
import pandas as pd

def daily_topic_classification(post, topics):
    # Initialize empty dataframe
    thread = pd.DataFrame(columns=['thread_id', 'topic'])
    
    # Classify each thread into a topic
    for index, thread_id in enumerate(post.thread_id.unique()):
        content = ' '.join([content for content in post[post.thread_id == thread_id]['clean_content']])
        thread_words = content.split()
        counter = [0]*len(topics)
        
        for topic_index, topic_words in enumerate(topics.values()):
            for topic_word in topic_words:
                counter[topic_index] += thread_words.count(topic_word)
        
        if len([i for i, v in enumerate(counter) if v == max(counter)]) == 1:
            thread.loc[index] = [thread_id, list(topics.keys())[np.argmax(counter)]]
        
        else:
            thread.loc[index] = [thread_id, None]
    
    return thread
    