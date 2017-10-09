"""
This file is proposed to delete
"""


from pymongo import MongoClient
from catalog_url_manipulators import get_resources
from ogd_mongo_handlers import create_catalog_db, create_resource_collection, insert_row_to_collection
from requests import get


def create_department_db(catalog_url):
    pass


def create_catalog_collection(resources):
    pass


def insert_row_to_collection(pay_load):
    pass


# import the text file of catalog_urls and load the urls to an array
with open() as url_data:
    catalog_urls = url_data.readlines()


# import the functions required to extract resources from a catalog_url


def maintain_pymongo(catalog_urls):
    # CATALOG STARTS
    for catalog_url in catalog_urls:
        # isolate the resources within the catalog
        resources = get_resources(catalog_url)['resources']
        create_catalog_db(catalog_url)

        # RESOURCE STARTS
        for resource in resources.keys():
            # get the embedded json file resources[key]['data_url']
            # json fields are dicts and json data is nested_lists
            # create a resourceful collection :)
            resource_json_data = get(resources[resource]['data_url']).content
            field_labels = [field['label']
                            for field in resource_json_data['fields']]
            create_resource_collection(resource)

            # RECORD STARTS
            # add the resource data
            for each in resource_json_data:
                resource_pay_load = {}
                for row in resource_json_data['data']:
                    for i in range(len(field_labels)):
                        pay_load[field_labels[i]] = row[i]
                insert_row_to_collection(resource_pay_load)
