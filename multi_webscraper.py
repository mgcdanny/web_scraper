#coding: utf-8
import queue
import sqlite3
import concurrent.futures
import threading
import requests as rq
from helper.help import create_logger, create_db, fill_queue

def crawl_task():
    try:
        url = Q.get()
        print(url)
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
    """ """
    # 60 bc i know how many are in the queue
    max_workers = 60
    with concurrent.futures\
            .ThreadPoolExecutor(max_workers=max_workers) as crawl_threads:
                [crawl_threads.submit(crawl_task) for i in range(0, Q.qsize())]

if __name__ == '__main__':
    DB_NAME = "example.db"
    TBL_NAME = "multi_web"
    create_db(DB_NAME, TBL_NAME, {'url': 'text', 'data': 'text'})
    Q = queue.Queue()
    #put 60 kayak pages into the queue to scrape
    urls = [link for link 
             in ["http://www.kayak.com/flights/NYC-HNL/2014-{month}-{day}"
             .format(month=month, day=day) 
             for day in range(1, 31) for month in [11, 12]]]
    #Thread safe container
    fill_queue(Q,urls)
    logger = create_logger(save_file="log/multi_log.log", mode='w')
    main()

