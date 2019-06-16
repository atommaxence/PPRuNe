#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun  2 09:06:13 2019

@author: maxence.faldor
"""

import pandas as pd
from textblob import TextBlob
import re

def daily_sentiment_analysis(post):
    def clean_sentiment(text):
        return re.sub(r'(\[QUOTE=.*?\]|\n|\r|\.{2,}|-{2,})', ' ', text)

    def get_polarity(text):
        return TextBlob(text).sentiment.polarity
    
    def get_subjectivity(text):
        return TextBlob(text).sentiment.subjectivity
    
    post['polarity'] = post.apply(lambda row: get_polarity(clean_sentiment(row['content'])), axis=1)
    post['subjectivity'] = post.apply(lambda row: get_subjectivity(clean_sentiment(row['content'])), axis=1)
    return post