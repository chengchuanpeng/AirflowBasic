import time
import re
from bs4 import BeautifulSoup
from selenium import webdriver
import jieba
import numpy as np
from sklearn.feature_extraction.text import TfidfVectorizer
import urllib3
from urllib3.exceptions import InsecureRequestWarning

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.sqlite_operator import SqliteOperator
urllib3.disable_warnings(InsecureRequestWarning)

# https://airflow.apache.org/docs/stable/macros-ref.html

start_command = """
    {% for i in range(2) %}
        echo "{{ task }}"
        echo "{{ ts }}"
        echo "{{ ds }}"
        echo "deargs: {{ dag.default_args['start_date_str'] }}" 
        echo "{{ params.my_param }}"
    {% endfor %}
"""

def crawl_page_links(forum, current_date, start_date_str):
    
    LINK_HEADER = 'https://www.ptt.cc/bbs/'
    driver = webdriver.Chrome()
    page_links = []
    driver.get(LINK_HEADER+forum+'/index.html')
    soup = BeautifulSoup(driver.page_source, 'lxml')
    latest_idx = int(re.findall('\d+', soup.find_all(class_ = 'btn wide')[1].get('href'))[0])
    

    print('current_date_for_task:{0}'.format(current_date))
    print('start_date_str:{0}'.format(start_date_str))
    interval = datetime.strptime(current_date, "%Y-%m-%d") - datetime.strptime(start_date_str, "%Y-%m-%d")
    interval = interval.days
    crawl_page = latest_idx - interval
    #if interval < latest_idx:
    driver.get(LINK_HEADER+forum+'/index'+str(crawl_page)+'.html')
    soup = BeautifulSoup(driver.page_source, 'lxml')
    page_links.extend(get_article_links(soup, forum))
    time.sleep(5)

    driver.quit()

    return page_links

def get_article_links(soup, forum):
    
    article_links = []
    for entry in soup.select('.r-ent'):
        article = {}
        if '(本文已被刪除)' in entry.select('.title')[0].text.strip(): continue
        if len(entry.select('a')) == 0: continue
        article_links.append(entry.select('a')[0].get('href').strip())
    return article_links

def get_article_content(**context):

    LINK_HEADER2 = 'https://www.ptt.cc/'
    driver = webdriver.Chrome()
    page_links = context['task_instance'].xcom_pull(task_ids='crawl_page')
    article_content = []
    for link in page_links:
        driver.get(LINK_HEADER2+link)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        main_content = soup.find(id="main-content")
        filtered = []
        for v in main_content.stripped_strings:
            if '發信站: 批踢踢實業坊(ptt.cc), 來自: ' in v: break
            if v[0] not in [u'※', u'◆'] and v[:2] not in [u'--']:
                filtered += [v]
        
        expr = re.compile(r'[^\u4e00-\u9fa5\u3002\uff1b\uff0c\uff1a\u201c\u201d\uff08\uff09\u3001\uff1f\u300a\u300b\s\w:/-_.?~%()]|jpg|imgur|com |from|com|https|http|png|\d+')
        for i in range(len(filtered)):
            filtered[i] = re.sub(expr, '', filtered[i])
            filtered[i] = re.sub(r'(\s)+', ' ', filtered[i])

        # remove empty strings
        filtered = [ f for f in filtered if f]  
        article = ' '.join(filtered)
        article_content.append(article)
    
    driver.quit()

    return article_content

def tf_idf(**context):

    article_content = context['task_instance'].xcom_pull(task_ids='get_article')
    corpus = []
    for article in article_content:
        corpus.append(" ".join(jieba.cut(article, cut_all=False)))
    
    vectorizer = TfidfVectorizer()
    tfidf_mat = vectorizer.fit_transform(corpus)
    words = vectorizer.get_feature_names()
    weight = tfidf_mat.toarray()
    feat = np.argsort(-weight)
    keywords = ""
    for article_feat in feat:
        keywords += ' '.join([ words[article_feat[k]] for k in range(5)])
        keywords +=','
    
    for article_keyword in keywords:
        if '賓士' or 'Benz' or '車' in keywords:
            keywords = str(keywords).replace("'", "")
            context['task_instance'].xcom_push(key='keywords', value = keywords)
            return 'record_in_db'
            #return ['record_in_db', 'do_something']
    
    return 'do_nothing'


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 5),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'daily' : 1,
    'retry_delay': timedelta(minutes=5),
    'start_date_str': "2020-06-05"
}

with DAG(dag_id='crawl_ptt', default_args=default_args) as dag:

    # Add Operator
    start = BashOperator(
    task_id='start',
    bash_command=start_command,
    params={'my_param': 'Start crawl page from ppt'},
    )

    crawl_page = PythonOperator(
        task_id='crawl_page',
        python_callable=crawl_page_links,
        op_args=['car', "{{ds}}", "{{ dag.default_args['start_date_str'] }}"],
        provide_context=False
    )

    get_article = PythonOperator(
        task_id='get_article',
        python_callable=get_article_content,
        provide_context=True
    )

    get_keywords = BranchPythonOperator(
        task_id='get_keywords',
        python_callable=tf_idf,
        provide_context=True
    )
    
    record_in_db = SqliteOperator(
        task_id='record_in_db', 
        sqlite_conn_id="sqlite_default", 
        sql="INSERT INTO KEYWORD (Forum, KeywordList, FinishTimestamp) VALUES ('{fourm}', '{keywords}', '{timestamp}');".
        format(fourm='car', keywords="{{ ti.xcom_pull(task_ids='get_keywords', key='keywords') }}", timestamp='{{ts}}')
    )

    do_nothing = DummyOperator(task_id='do_nothing')
    do_something = DummyOperator(task_id='do_something')

    start >> crawl_page >> get_article >> get_keywords 
    get_keywords >> record_in_db
    get_keywords >> do_nothing
    #get_keywords >> do_something