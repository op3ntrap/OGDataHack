import requests
from bs4 import BeautifulSoup
from threading import Thread


def get_catalog_urls(link):
    r = requests.get(link)
    soup = BeautifulSoup(r.content, "lxml")
    # first find all the view class divs
    for tags in soup.find_all('div', 'views-field views-field-title'):
        for tag in tags.find_all('a'):
            if "/catalog/" in tag.get('href'):
                print tag.get('href')
    for i in range(1, 471):
        get_catalog_urls("https://data.gov.in/catalogs?sort_by=created&sort_order=DESC&items_per_page=9&page=" + str(i))


def load_urls():
    with open("/home/risan/catalog_urls.txt", 'rw') as catalog_urls:
        urls_data = catalog_urls.readlines()


def get_json_resources(catalog_url):
    r = requests.get(catalog_url)
    soup = BeautifulSoup(r.content, "lxml")
    # total_resources = int(soup.find('div', 'view-header').text)
    resources = {}

    resource_urls = [tag.get('href') for tag in soup.find_all('a', 'json')]
    if len(resource_urls) == 0:
        resources[soup.find('div', id='breadcrumb-container').find('span', 'active').text] = {
            'content': "",
            'json_file_url': "",
        }
        return resources
    # print resource_urls
    resource_titles = [tag.text for tag in soup.find_all('div', 'views-field views-field-title')]
    # print resource_titles
    resource_briefs = [tag.text for tag in soup.find_all('div', 'field-content ogpl-more')]

    for i in range(len(resource_urls)):
        resources[resource_titles[i + 10]] = {
            'content': resource_briefs[i],
            'json_file_url': resource_urls[i],
        }
    # print "resource", resources
    return resources


def get_catalog_resources(catalog_url):
    catalog_data = {
        'catalog_title': "",
        'resources': {}
    }

    r = requests.get(catalog_url)
    soup = BeautifulSoup(r.content, "lxml")
    total_resources = int(soup.find('div', 'view-header').text.strip().split(" ")[0])
    # print total_resources
    # add the catalog title to the data
    catalog_data['catalog_title'] = soup.find('div', id='breadcrumb-container').find('span', 'active').text
    if total_resources <= 6:
        catalog_data['resources'] = get_json_resources(catalog_url)
        return catalog_data
    else:
        count = 0
        resources = []

        while total_resources > 0:

            if count == 0:
                resource = (get_json_resources(catalog_url))
                resources.append(resource)
                count += 1
                total_resources -= len(resource.keys())
                continue

            resource = get_json_resources(catalog_url + "?title=&file_short_format=&page=" + str(count))
            resources.append(resource)
            total_resources -= len(resource.keys())
            count += 1
        for val in resources:
            # print val.keys()
            for key in val.keys():
                catalog_data['resources'][key] = val[key]
        return catalog_data


r = get_catalog_resources("https://data.gov.in/catalog/surat-citizen-centric-services")
print r['catalog_title']
for key, val in r['resources'].iteritems():
    print "\n\n\n\n"
    print key
    print "\n\n\n\n"
    print val


# print r['resources']
def get_json_save():
    pass
