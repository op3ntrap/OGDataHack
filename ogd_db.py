from pymongo import MongoClient
import json
import requests
from bs4 import BeautifulSoup
from sector_scraping import get_catalog_urls_
"""
Database Engine: Mongodb
Database Connector: Pymongo
Database Schema:
|--Ministry || State {DB}
	|--Catalogs {Collection}
		|--Resources {Document}
			|--Sectors {Attribute}
}
"""
mongo = MongoClient(host=localhost, port=8090)


def get_heirarchy(url):
	'''
	Method to extract heirarchy list from urls
	1.Central Catalogs
	2.State Catalogs
	param: url
	return: (dict) levels heirarchy
	'''
	r = requests.get(url)
	soup = BeautifulSoup(r.content, "lxml")
	# soup = BeautifulSoup(r.content, "html5lib")
	tb = soup.find('tbody')  # Find the data table
	ele = tb.find_all('tr')  # Make a list of all elements in first column
	count = 0  # initialising the index of ele (list)
    ele.pop()# remove the element 'total from the list'
	levels = {}# heirarchial dict 
	ministry_titles = [{val.find('a').get('title'):val.find('a').get('href')} for val in ele]
	for val in ele:
	    level = int(val.get('class')[0].split('-')[1])
	    if level == 1:# Create a main entry if the level is 1
	        levels[count] = {
	        					'title': val.find('a').get('title'),
	        					'url': val.find('a').get('href'),
	        					'total_resources': val.find_all('td')[-2].text,
	        					'total_catalogs': val.find_all('td')[-1].text,
	        					'subclasses': {}
	        				}
	        count+=1
	        continue
	    if level == 2:# Create an entry for the last added element in level 1
	        d = levels.keys()
	        levels[d[-1]]['subclasses'][count] = {
	        					'title': val.find('a').get('title'),
	        					'url': val.find('a').get('href'),
	        					'total_resources': val.find_all('td')[-2].text,
	        					'total_catalogs': val.find_all('td')[-1].text,
	        					'subclasses': {}
	        					}
	        count+=1
	        continue
	    if level == 3:# Inclusion
	        d = levels.keys()
	        d1 = levels[d[-1]].keys()
	        levels[d[-1]][d1[-1]]['subclasses'][count] = {
	        					'title': val.find('a').get('title'),
	        					'url': val.find('a').get('href'),
	        					'total_resources': val.find_all('td')[-2].text,
	        					'total_catalogs': val.find_all('td')[-1].text,
	        					'subclasses': {}
	        					}
	        count += 1
	        continue
	return {'department_titles':ministry_titles,'levels':levels}

def create_department_db(department_titles):
	"""
	Initialise a list of MongoDB database connections
	"""
	mongo_department_db = []# list of db connections
	for title in department_titles:
		dbh = mongo[title.keys()[0].replace(" ",",")
		mongo_department_db.append(dbh)# create databases in the Mongo Instance
	print "State or Central Ministry Databases have been created"

def create_collections(department):
	mongo_catalog_collections = []
	for val in department:
		result = get_catalogs(val.values()[0])
		catalogs = result['catalogs'].keys()
		db = val.keys()[0].replace(" ", "-")
		for name in catalogs:
			r = db[name.replace(" ",",")]
	return
	
def update_resources(catalog):
	# TODO import the resource scraper with a safety check for availability for json data set
	pass


