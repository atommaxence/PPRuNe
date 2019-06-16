#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 29 09:35:26 2019

@author: maxence.faldor
"""

# Import
import sys
import requests
from bs4 import BeautifulSoup
import pandas as pd
from sqlalchemy import create_engine, MetaData, select, func, Table, text
import time
import datetime
import dateparser
import re

# Define useful functions
def get_out_of_date_threads(up_to_date, forum_info):
    out_of_date_threads = []
    
    r = requests.get('https://www.pprune.org/{}?pp=200'.format(forum_info[4]))
    soup = BeautifulSoup(r.text, 'html.parser')
    
    # Find first normal thread
    soup_thread = soup.find_all('div', {'class': 'trow thead threadbit'})
    soup_thread = soup_thread[-1].find_next('div', {'class': 'trow text-center'})
    
    
    while True:
        blocks = soup_thread.find_all('div', recursive=False)
        
        # Verify if post is moved
        img = blocks[0].find('img')
        if img['src'] == 'https://www.pprune.org/images/statusicon/thread_moved.gif':
            soup_thread = soup_thread.find_next('div', {'class': 'trow text-center'})
            continue
        
        # Get url
        link = blocks[2].find('h4', {'class': 'style-inherit', 'style': 'display: inline;'}).find('a', href = True)
        url = link['href']
        
        # Get date
        date_block = blocks[3].get_text().split('by')
        date = dateparser.parse(date_block[0].strip())
            
        if date <= up_to_date:
            break
        
        # Parse thread number
        m = re.search('www.pprune.org/{}(\d+?)-'.format(forum_info[5]), url)
        if m:
            thread_number = m.group(1)
            archive_url = 'https://www.pprune.org/archive/index.php/t-' + thread_number + '.html'
            
            # Append archive url
            out_of_date_threads.append(archive_url)
        
        # Get next thread
        try:
            soup_thread = soup_thread.find_next('div', {'class': 'trow text-center'})
        except:
            break
        
    return out_of_date_threads

def get_out_of_date_posts(url, up_to_date):
    thread = pd.DataFrame(columns=['name', 'url'])
    
    post = pd.DataFrame(columns=['user', 'date', 'content'])
    index_post = 0
    
    r = requests.get(url)
    soup = BeautifulSoup(r.text, 'html.parser')
    
    name = soup.find('p', {'class': 'largefont'}).find('a').get_text()
    thread.loc[0] = [name, url]

    try:
        page_number_thread_max = int(soup.find('div', {'id': 'pagenumbers'}).find_all('a')[-1].get_text())
    except:
        page_number_thread_max = 1

    page_number_thread = 1
    while True:
        time.sleep(3)
        soup_posts = soup.find_all('div', {'class': 'post'})

        for soup_post in soup_posts:
            user = soup_post.find('div', {'class': 'username'}).get_text()
            date = dateparser.parse(soup_post.find('div', {'class': 'date'}).get_text())
            content = soup_post.find('div', {'class': 'posttext'}).get_text().encode('ascii', 'ignore').decode('utf-8', errors = 'surrogatepass')
            
            if date > up_to_date:
                post.loc[index_post] = [user, date, content]
                index_post += 1

        if page_number_thread == page_number_thread_max:
            break
        else:
            page_number_thread += 1
            r = requests.get(url[:-5] + '-p-' + str(page_number_thread) + '.html')
            soup = BeautifulSoup(r.text, 'html.parser')
    
    return thread, post

# Get script arguments
if len(sys.argv) < 7:
    print('Usage: python3 pprune_daily_scraping.py <dbip> <dbport> <dbusername> <dbpassword> <dbschema> <forum_id>')
    sys.exit(1)

dbip = sys.argv[1]
dbport = int(sys.argv[2])
dbusername = sys.argv[3]
dbpassword = sys.argv[4]
dbschema = sys.argv[5]
forum_id = int(sys.argv[6])

# Configure connection to database
engine = create_engine('hana+pyhdb://{}:{}@{}:{}'.format(dbusername, dbpassword, dbip, dbport))
metadata = MetaData(engine)

el_pprune_forum = Table('el_pprune_forum', metadata,  schema=dbschema, autoload=True)
el_pprune_thread = Table('el_pprune_thread', metadata,  schema=dbschema, autoload=True)
el_pprune_post = Table('el_pprune_post', metadata,  schema=dbschema, autoload=True)

forum_info = engine.execute(select([el_pprune_forum]).where(el_pprune_forum.columns.forum_id == forum_id)).fetchall()[0]

query = """select max(DATE) from 
(select THREAD."FORUM_ID",
POST."POST_ID",
POST."THREAD_ID",
POST."DATE" as "DATE"
from "{}"."EL_PPRUNE_POST" POST left outer join "EL_PPRUNE_THREAD" THREAD on POST."THREAD_ID" = THREAD."THREAD_ID"
where THREAD."FORUM_ID" = {})""".format(dbschema, forum_id)
up_to_date = engine.execute(text(query)).fetchall()[0]._row[0]

for url in get_out_of_date_threads(up_to_date, forum_info):
    result = engine.execute(select([el_pprune_thread.columns.thread_id, el_pprune_thread.columns.url]).where(el_pprune_thread.columns.url == url)).fetchall()
    
    if len(result) == 1:
        thread_id = result[0][0]
        result = engine.execute(select([func.max(el_pprune_post.columns.date)]).where(el_pprune_post.columns.thread_id == thread_id)).fetchall()
        up_to_date = result[0][0]
        
        thread, post = get_out_of_date_posts(url, up_to_date)
        
        # Add new posts to database associated with the existing thread
        next_post_id = engine.execute(func.max(el_pprune_post.columns.post_id)).fetchall()[0]._row[0] + 1
        
        post['post_id'] = next_post_id + post.index
        post['thread_id'] = thread_id
        post['created_by'] = dbusername
        post['created_date'] = datetime.datetime.now()
        
        post.to_sql('el_pprune_post', con=engine, if_exists='append', schema=dbschema, index=False)
    
    else:
        thread, post = get_out_of_date_posts(url, datetime.datetime(1337, 4, 20))
        
        # Add new thread
        next_thread_id = engine.execute(func.max(el_pprune_thread.columns.thread_id)).fetchall()[0]._row[0] + 1
        next_post_id = engine.execute(func.max(el_pprune_post.columns.post_id)).fetchall()[0]._row[0] + 1
        
        thread['thread_id'] = next_thread_id
        thread['forum_id'] = forum_id
        thread['created_by'] = dbusername
        thread['created_date'] = datetime.datetime.now()
        
        post['post_id'] = next_post_id + post.index
        post['thread_id'] = next_thread_id
        post['created_by'] = dbusername
        post['created_date'] = datetime.datetime.now()
        
        thread.to_sql('el_pprune_thread', con=engine, if_exists='append', schema=dbschema, index=False)
        post.to_sql('el_pprune_post', con=engine, if_exists='append', schema=dbschema, index=False)

