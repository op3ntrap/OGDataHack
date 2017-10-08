import json
import requests
from bs4 import BeautifulSoup
from resource_scraper import get_catalog_urls
from threading import Thread
from time import sleep
from tqdm import tqdm


def sector_links():
    """
    extract the sector link endpoints from the sector list homepage
    :return (list) of sector endpoints eg "/sector/Agriculture-9212":
    """
    sectors_homepage = requests.get("https://data.gov.in/sectors")
    soup = BeautifulSoup(sectors_homepage.content, "lxml")
    # <div class="views-field views-field-nothing">        <span class="field-content">
    div_elements = soup.find_all('div', 'views-field views-field-nothing')
    # links in the div classes
    links = [element.find('a').get('href') for element in div_elements]
    return links


def get_catalogs_sector_wise(sector_url, ):
    """
    Function to Extract
    1. No. of Catalogs per Sector
    2. URLs of Catalogs in each sector
    :param sector_url:
    :return (list) Catalog URLs in each sector:
    """
    sleep(0.2)
    trunk = 'https://data.gov.in/'
    # TODO Prepare Proper Documentation for these functions
    sector = {
        'name': sector_url.split('-')[0].split('/')[-1],
        'url': trunk + sector_url,
        'catalogs': {}
    }
    sector_html = requests.get(trunk + sector_url)  # http request
    soup = BeautifulSoup(sector_html.content, "lxml")
    total_pages = 0
    try:
        pagination_list = soup.find('li', 'pager-last last')
        pagination_trunk = pagination_list.find('a').get('href')[:-2]
    except AttributeError:
        total_pages = 1
        pagination_trunk = sector_url
    if total_pages != 1:
        try:
            total_pages = int(pagination_list.find('a').get('href')[-2:])
        except ValueError:
            total_pages = int(pagination_list.find('a').get('href')[-1])
    # HTML Snippet
    # <div class="views-field views-field-title">
    #   <span class="field-content">
    #       <a href="/catalog/physical-performance-districts-based-sample-survey">
    #           Physical Performance of the Districts - Based on Sample Survey
    #       </a>
    print "Downloading " + sector['name']
    for page in tqdm(range(total_pages + 1)):
        if page == 0:
            sector['catalogs'].update(get_catalog_urls(trunk + sector_url))  # http request

        else:
            sector['catalogs'].update(get_catalog_urls(trunk + pagination_trunk + str(page)))  # http request
    with open(sector['name'] + str('.json'), 'w+') as output_load:
        json.dump(sector, output_load)
    return 0


if __name__ == "__main__":
    # Accumulate all the sector url endpoints in a Python List
    sector_url_endpoints = sector_links()
    # TODO optimise the multithreading capabilities of this piece of function
    for i in range(0, 32, 3):
        thrds = [Thread(target=get_catalogs_sector_wise, args=(sector_url_endpoints[i + x],)) for x in range(3)]
        start_thrds = [t.start() for t in thrds]
        collect_thrds = [t.join() for t in thrds]
