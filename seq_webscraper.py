#coding: utf-8
import queue
import sqlite3
import logging
import sqlite3
from selenium import webdriver
from helper.help import create_logger, create_db

logger = create_logger(save_file="log/seq_log.log",mode='w')
create_db('example.db','web_results_seq',{'url':'text','data':'text'})

# Page to scrape
# DOMAIN = 'http://www1.nyc.gov'
# SUB_DOMAIN = '/office-of-the-mayor/news.page'
DOMAIN = 'http://www.danielgabrieli.info/'
SUB_DOMAIN = ''
# Initial seeding of queue for sites to scrape
Q = [DOMAIN+SUB_DOMAIN]
VIEWED_LINKS = set()

def crawl_task():
	if not Q:
		return False
	url = Q.pop(0)
	conn = sqlite3.connect('example.db')
	driver = webdriver.PhantomJS()
	try:
		logger.info("[%s] crawling url [%s] ..." % ("main thread", url))
		driver.get(url)
		links = [tag.get_attribute('href') for tag in driver.find_elements_by_tag_name('a')]
		for link in links:
			if link:
				visited = link in VIEWED_LINKS
				if (not visited) and (link.startswith(DOMAIN + SUB_DOMAIN)):
					Q.append(link)
					VIEWED_LINKS.add(link)
		conn.execute('''INSERT INTO web_results_seq VALUES(?,?)''',(url, driver.page_source))
		conn.commit()
	except Exception as e:
		print(e)
		logging.error('crawl task error {}'.format(e))
	finally:
		driver.quit()
		conn.close()
		return True

def main():
	cont = True
	while cont:
		cont = crawl_task()

if __name__ == '__main__':
    main()





