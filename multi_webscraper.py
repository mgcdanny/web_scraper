#coding: utf-8
import logging
import queue
import concurrent.futures
import sqlite3
import threading
import requests as rq

DB_NAME = "example.db"
TBL_NAME = "multi_web"


def create_logger(save_file, mode='a'):
    """Create logger with some default settings.
    Writes output to save_file"""
    logger = logging.getLogger("spider")
    logger.setLevel(logging.DEBUG)
    h1 = logging.FileHandler(save_file, mode)
    f1 = logging.Formatter("%(asctime)s - %(lineno)d - %(message)s")
    h1.setFormatter(f1)
    logger.addHandler(h1)
    return logger


def create_db(db_name, table_name, cols):
    """Init db helper function
    TODO: reserach check_same_thread sqlite3 parameter"""
    conn = sqlite3.connect(db_name)
    conn.execute('''DROP TABLE IF EXISTS {}'''.format(table_name))
    conn.execute('''CREATE TABLE {table} ({cols})'''
        .format(table=TBL_NAME,
                cols=', '.join([' '.join([k, cols[k]]) for k in cols])))
    conn.commit()
    conn.close()

logger = create_logger(save_file="log/multi_log.log", mode='w')
create_db(DB_NAME, TBL_NAME, {'url': 'text', 'data': 'text'})

#Thread safe container
Q = queue.Queue()

#put 60 kayak pages into the queue to scrape
[Q.put(link) for link
    in ["http://www.kayak.com/flights/NYC-HNL/2014-{month}-{day}"
        .format(month=month, day=day)
        for day in range(1, 31) for month in [11, 12]]]


def crawl_task():
    try:
        url = Q.get()
    except queue.Empty:
        return None
    conn = sqlite3.connect(DB_NAME)
    try:
        logger.info("[{}] crawling url [{}] ..."
                    .format(threading.current_thread().name, url))
        html_raw = rq.get(url).text
        conn.execute('''INSERT INTO {} VALUES(?,?)'''.format(TBL_NAME),
                    (url, html_raw))
        conn.commit()
    except Exception as e:
        print(e)
        logging.error('crawl task error {}'.format(e))
    finally:
        conn.close()


def main():
    # 60 bc i know how many are in the queue
    max_workers = 60
    with concurrent.futures\
            .ThreadPoolExecutor(max_workers=max_workers) as crawl_threads:
                [crawl_threads.submit(crawl_task) for i in range(0, Q.qsize())]

if __name__ == '__main__':
    main()
