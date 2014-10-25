#coding: utf-8
import logging
import queue
import concurrent.futures
import sqlite3
import threading
from selenium import webdriver


DB_NAME = "example.db"
TBL_NAME = "multi_crawler"


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

logger = create_logger(save_file="log/multi_crawler.log", mode='w')
create_db(DB_NAME, TBL_NAME, {'url': 'text', 'data': 'text'})


# Page to scrape
# DOMAIN = 'http://www1.nyc.gov'
# SUB_DOMAIN = '/office-of-the-mayor/news.page'
DOMAIN = 'http://www.danielgabrieli.info/'
SUB_DOMAIN = ''
# Initial seeding of queue for sites to scrape
Q = queue.Queue()
Q.put(DOMAIN+SUB_DOMAIN)

LOCK = threading.Lock()
VIEWED_LINKS = set()


def crawl_task(lock):
    try:
        url = Q.get(block=True, timeout=.5)
    except queue.Empty:
        return None
    conn = sqlite3.connect('example.db')
    driver = webdriver.PhantomJS()
    try:
        logger.info("[{}] crawling url [{}] ..."
                    .format(threading.current_thread().name, url))
        driver.get(url)
        links = [tag.get_attribute('href') for tag in driver.find_elements_by_tag_name('a')]
        with lock:
            for link in links:
                if link:
                    visited = link in VIEWED_LINKS
                    if (not visited) and (link.startswith(DOMAIN + SUB_DOMAIN)):
                        Q.put(link)
                        VIEWED_LINKS.add(link)
        conn.execute('''INSERT INTO web_results VALUES(?,?)''',(url, driver.page_source))
        conn.commit()
    except Exception as e:
        print(e)
        logging.error('crawl task error {}'.format(e))
    finally:
        driver.quit()
        conn.close()

def main():
    max_workers = 20
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as crawl_threads:
            while not Q.empty():
                futures_container = [crawl_threads.submit(crawl_task, LOCK) for i in range(0,min(Q.qsize(),max_workers))]
                concurrent.futures.wait(futures_container)

if __name__ == '__main__':
    main()





