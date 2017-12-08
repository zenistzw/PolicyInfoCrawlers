#!/usr/bin/python3
# -*- coding: utf-8 -*-
import logging
logging.basicConfig(level=logging.DEBUG,  
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',  
                    datefmt='%a, %d %b %Y %H:%M:%S',  
                    filename='mof.log',
                    filemode='a')
# import grequests
import os
import re
import requests
import sys
import time
from bs4 import BeautifulSoup
from urllib.parse import urlparse

# from mysql import MySQL

"""
@author: SummyChou
@target: 财政部 - 政务信息 - 政策解读 专栏数据抓取
"""

ZC_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Connection': 'keep-alive',
    'Host': 'www.mof.gov.cn',
    'Referer': 'http://www.mof.gov.cn/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
}

ZCJD_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, sdch',
    'Accept-Language': 'zh-CN,zh;q=0.8',
    'Connection': 'keep-alive',
    'Host': 'www.mof.gov.cn',
    'Referer': 'http://www.mof.gov.cn/zhengwuxinxi/zhengcejiedu/',
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.157 Safari/537.36'
}

PROXY_POOL = [
    {'https': ''},
]

POLICY_LIST = []

CURRENT_DATE = time.strftime('%Y-%m-%d', time.localtime(time.time()))





def callback_function_zc(res, **kwargs):
    if not res.status_code == 404:
        logging.info('****************************************')
        logging.info('URL请求成功 -- {}'.format(res.url))
        id = urlparse(res.url)[2].split('/')[-1].split('.')[0]
        try:
            with open('txt/{0}/{1}.txt'.format(CURRENT_DATE, id), 'w') as fd:
                res.encoding = 'utf-8'
                soup = BeautifulSoup(res.content, 'lxml')
                ps = soup.find(name='td', attrs={'id': 'Zoom'}).find_all(name='p')
                for p in ps:
                    fd.write(p.get_text().strip())
        except Exception as e:
            logging.error('页面解析错误 -- {}'.format(res.url))
            logging.error(e)
            sys.exit(1)


def exception_handler(req, exception):
    logging.error('URL请求失败 -- {}'.format(req.url))
    logging.error(exception)


def callback_function_for_zcjd(res, **kwargs):
    global POLICY_LIST
    logging.info('****************************************')
    logging.info('URL请求成功 -- {}'.format(res.url))
    
    try:
        res.encoding = 'utf-8'
        soup = BeautifulSoup(res.text, 'lxml')
        tds = soup.find(name='table', attrs={'id': 'id_bl', 'class': 'ZIT'}).find_all(name='td')
        for td in tds:
            link = td.find(name='a')['href'].strip()
            if link.startswith('./'):
                link = 'http://www.mof.gov.cn/zhengwuxinxi/zhengcejiedu' + link[1:]
            elif link.startswith('../') and not link.startswith('../../'):
                link = 'http://www.mof.gov.cn/zhengwuxinxi' + link[2:]
            elif link.startswith('../../'):
                link = 'http://www.mof.gov.cn' + link[5:]
            logging.info('新增一条记录的链接 --')
            logging.info(link)
            id = urlparse(link)[2].split('/')[-1].split('.')[0]
            title = td['title'].strip()
            date = title[-12:][1:][:-1]
            title = title[:-13]
            POLICY_LIST.append((date, title, id, link))
    except Exception as e:
        logging.error('页面解析错误 -- {}'.format(res.url))
        logging.error(e)
        sys.exit(1)

def main():
    try:
        url = 'http://www.mof.gov.cn/zhengwuxinxi/zhengcejiedu/'
        res = requests.get(url=url, headers=ZCJD_HEADERS)
        logging.info('URL请求成功 -- {}'.format(url))
        num_of_pages = int(re.findall(r'var countPage = (\d+)', res.text, re.S)[0])
    except Exception as e:
        logging.error('URL请求失败 -- {}'.format(url))
        logging.error(e)
        sys.exit(1)
    
    zcjd_request_urls = ['http://www.mof.gov.cn/zhengwuxinxi/zhengcejiedu/index_%d.htm' % i for i in range(1, num_of_pages)]
    zcjd_request_urls = ['http://www.mof.gov.cn/zhengwuxinxi/zhengcejiedu/index.htm'] + zcjd_request_urls

    # os.mkdir('html/{}'.format(CURRENT_DATE))
    with requests.Session() as session:
        for url in zcjd_request_urls:
            try:
                res = session.get(url=url, headers=ZCJD_HEADERS, hooks=dict(response=callback_function_for_zcjd))
                '''
                持久化存储
                '''
                # TODO
            except Exception as e:
                logging.error('URL请求失败 -- {}'.format(url))
                logging.error(e)
                sys.exit(1)
    
    # new_conn = MySQL(username='root', password='yunan0808')
    # new_conn.connect_to_database_server()
    # new_conn.change_database('PolicyInfo')
    # # sql = 'INSERT INTO mof(policy_date, policy_title, policy_id, policy_link) VALUES(%s, %s, %s, %s);'
    # # new_conn.commit_to_database(sql, POLICY_LIST)
    # sql = 'SELECT policy_link FROM mof;'
    # results = new_conn.query_from_database(sql)
    # new_conn.disconnect_from_database_server()

    # os.mkdir('txt/{}'.format(CURRENT_DATE))
    # url_queue = (link[0] for link in results)
    # req_queue = (grequests.get(url=url, headers=ZC_HEADERS, hooks=dict(response=callback_function_zc)) for url in url_queue)
    # grequests.map(req_queue, stream=True, size=10, exception_handler=exception_handler)

if __name__ == '__main__':
    sys.exit(int(main() or 0))
    