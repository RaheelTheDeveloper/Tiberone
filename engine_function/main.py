import pandas as pd
import numpy as np
from fuzzywuzzy import process
from fuzzywuzzy import fuzz
from google.cloud import bigquery
from google.cloud.bigquery import DatasetReference
import json
import os
from google.oauth2 import service_account
from flask import Flask, request

app=Flask("test")
@app.route("/")
def my_engine_function(request):
    productToADD = ''
    productname = ''

    credentials = service_account.Credentials.from_service_account_file('tiberone-gcp-lab-7b278010e9c0.json')
    project_id = 'tiberone-gcp-lab'
    table_id = "tiberone-gcp-lab.CatalogEngine.RawData"
    client = bigquery.Client(credentials= credentials,project=project_id)

    ### Raw Data
    sample_count = 2000
    row_count_raw = client.query("""SELECT COUNT(*) as total FROM tiberone-gcp-lab.CatalogEngine.RawData""").to_dataframe().total[0]
    Not_Verified_Data = client.query("""SELECT * FROM tiberone-gcp-lab.CatalogEngine.RawData WHERE RAND() < %d/%d""" % (sample_count, row_count_raw)).to_dataframe()

    ### Catalog Data
    sample_count = 2000
    row_count_catalog = client.query("""SELECT COUNT(*) as total FROM tiberone-gcp-lab.CatalogEngine.CatalogData""").to_dataframe().total[0]
    Product_Cat_Data = client.query("""SELECT * FROM tiberone-gcp-lab.CatalogEngine.CatalogData WHERE RAND() < %d/%d""" % (sample_count, row_count_raw)).to_dataframe()

    product_catalog =  Product_Cat_Data['Catalog_names']
    product_id = Product_Cat_Data['Catalog_Id']
    raw_data = Not_Verified_Data['Raw_Names']
    print('Engine Started...')

    ##Match Items Logic
    execTime = 1
    limit = 5

    query = raw_data
    choices = product_catalog
    choices_id = product_id

    results = []
    list_of_lists = [[] for i in range(execTime*limit)]


    ##Mapping the products to the product id
    zip_iterator = zip(choices, choices_id)
    catalog_dic = dict(zip_iterator)

    ##To Match the strings (Fuzzy Logic)
    iter = 0
    iter1 = 0

    for i in range(execTime): ##How many products you want to see
        results = process.extract(query[i], choices, limit=limit)
        for j in range(0, 5):
            print("Raw product =>", query[iter], " ", ",match =>", results[j][0], " ", ",matching percentage =>", results[j][1])

            list_of_lists[iter1].insert(0,results[j][1])
            list_of_lists[iter1].insert(0,results[j][0])
            list_of_lists[iter1].insert(0,query[iter])

            iter1 = iter1 + 1

        iter = iter + 1

    print(list_of_lists)


    #ls = list_of_lists[0]
    #json_str = json.dumps(ls)
    #print(ls)
    #print(json_str)
    #return json_str
    myjson = json.dumps(list_of_lists)
    return myjson

#if __name__ == '__main__':
#    app.run()
