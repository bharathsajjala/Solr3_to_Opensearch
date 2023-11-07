from __future__ import print_function
import argparse
from pydoc import resolve
from tracemalloc import start
from unicodedata import name
import pysolr
import requests
from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk
import time
import solrutil  as su
import urllib
from configparser import RawConfigParser
import pathlib
import urllib.parse
import opensearchutil as os
import solrcoreadmin as sa
import json
import pandas as pd
import time
import traceback
from redis import Redis
from hashlib import md5

from urllib.request import urlopen
import urllib.parse


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('--solr_host',
                        type=str,default="localhost")
    parser.add_argument('--solr_port',
                        type=str, default="8983")
    parser.add_argument('--solr_core',
                        type=str, default="")

    return vars(parser.parse_args())

r = None
def main():
    global r
    try:
        parser = RawConfigParser()
        parser.read('query_config.properties')
        solr_host = parser.get("solr", "host")
        solr_port = parser.get("solr", "port")
        solr_url_path=parser.get("solr", "path") 
        solr_core = parser.get("solr", "core")
        solr_username = parser.get("solr", "solr_username")
        solr_password = parser.get("solr", "solr_password")
       
        query =  parser.get("query", "q")
        queryString = parser.get("query", "qurystring")
        usequerystring = parser.get("query", "usequerystring")
        constr ="http://"+solr_host+":"+solr_port+"/"+solr_url_path+"/"
        result =""
        rc_solr = 0
        rc_search_solr = 0
        params = ""
        print("queryString",eval(usequerystring))
        try:
            r = Redis.from_url(url=parser.get("redis", "url"))
        except Exception as e:
            print(e)
        start_time = time.time()
        admin_url ="http://"+solr_host+":"+solr_port+"/"+solr_url_path+"/"
        cores_records = getAllCores(admin_url)
        cores = cores_records.keys()
        # print(cores)
        indexs = ["",""]
        for core in cores:
            try:
                # solr connection 
                auth = (solr_username,solr_password)
                con = su.connect(solr_host,auth,solr_url_path,solr_port,core)
                # constr ="http://lunr-sandbox.cernerasp.com:8080/CERNABCN_solr/patient-docs/"
                # con = pysolr.Solr(constr,auth=auth, always_commit=True, timeout=10)
                print(con.ping())
                if not eval(usequerystring) :
                    start =  parser.get("query", "start")
                    rows =  parser.get("query", "rows")
                    sort =  parser.get("query", "sort")
                    event = parser.get("query", "event")
                    securityUUID = parser.get("query", "securityUUID")
                    # search in solr
                    # result = con.search(query,securityUUID = securityUUID )
                    result = con.search("*:*",securityUUID = securityUUID , **{'rows': 100000})
                    print("count...")
                    print(len(result))
                else:
                    print("here",queryString)
                    params = urllib.parse.parse_qs(queryString)
                    result = con.search(query,**params)
                
            except Exception as e:
                print(e)
            finally:
                con.get_session().close() 
            rc_search_solr = len(result)
            actions = []
            op_type="index"
            index = core+"-index"
            docs = process(op_type, index, result)

            #opnesearch connection 
            host = parser.get("opensearch", "host")
            port = parser.get("opensearch", "port")
            user = parser.get("opensearch", "user")
            password = parser.get("opensearch", "password")
            end=0.0
            docs_cnt_before = 0
            try:
                client =os.connect(host,port, user,password)
                # print(client)
                # bulk import
                docs_cnt_before = getrcCount(client,index)
                print("docs_cnt_before",str(docs_cnt_before))
                print(docs)
                mr_succeeded, mr_failed = os.bulkupload(client, docs)
                print("rc_success....",mr_succeeded)
                print("rc_failed....",mr_failed)
                end = time.time()
                time_taken = float(end)-float(start_time)
                client =os.connect(host,port, user,password)
                docs_cnt_after = getrcCount(client,index)
                os_indexed_rc_count = (docs_cnt_after - docs_cnt_before)
                # print("rc_search_solr",str(rc_search_solr),"osdocs", (docs_cnt_after -docs_cnt_before))
                # assert rc_search_solr == os_indexed_rc_count
                os.saveResult(solr_host, solr_core,host ,index,cores_records.get(core), rc_search_solr,os_indexed_rc_count, mr_succeeded, mr_failed,query+"&"+queryString,time_taken)
            except Exception as e:
                print(e)     
            finally:
                client.close()

        solr_k = get_solr_keys(r,parser)   
        matched_rc, mismatched_rc = validateOs_docs(r,solr_k,parser,index)
        print("Valdated recods count : ",str(len(matched_rc)))
        print("Mismatched recods count : ",str(len(mismatched_rc)))
        print("Job completed")
    except Exception as e:
            print("An exception occurred:", e)
            traceback.format_exc()


def getAllJobruns():
    try:
            parser = RawConfigParser()
            parser.read('query_config.properties')
            r = Redis.from_url(url=parser.get("redis", "job_result_url"))
            for k in r.scan_iter('*'):
                print(r.get(k))
    except Exception as e:
            print(e)

def getrcCount(client , index):

    query = {
    "query": {
        "match_all": {}
        }
    }
    response = client.search(
    body=query,
    index=index)
    count = response["hits"]["total"]["value"]
    return count

def search_in_opensearch(client, index, query ):
    return os.search(client,index,query)

def process(op_type,index,docs):
    actions = []
    for doc in docs:
        h = md5()
        h.update(json.dumps(doc).encode('utf-8'))
        key = f'solr|{index}|{doc["documentId"]}'
        if r:
            hash = r.hget(key, "hash")
        new_hash = h.hexdigest()
        if hash is None or new_hash != hash.decode():
            if hash is not None:
                print(f'doc was updated: {hash.decode()} != {new_hash}')
            print(doc)
            if r:
                r.hset(key, "hash", new_hash)
                r.hset(key, "ts", time.time())
            print("uploading fields ",doc)
            actions.append({"_op_type": op_type, "_index": index, "_id":doc["documentId"], "_source": doc})
    return actions

def validateOpensearchIndex(client , index):

    query = {
    "query": {
        "match_all": {}
        }
    }
    response = client.search(
    body=query,
    index=index)
    count = response["hits"]["total"]["value"]
    return count
    
def validateOs_docs(r, solr_keys,parser,index):

    matched = []
    mismatched = []
    # get all keys from os and compare and add to list 
    host = parser.get("opensearch", "host")
    port = parser.get("opensearch", "port")
    user = parser.get("opensearch", "user")
    password = parser.get("opensearch", "password")
    client =os.connect(host,port, user,password)
    try:
        for k in r.scan_iter('*'):

            id = k.decode().split("|")[2]
            hash = r.hget(k, "hash")
            print("redis hash",hash)
            ts = r.hset(k, "ts", time.time())
        
            query = {
                        "query": {
                            "ids": {
                                "values": [id]}}}

            response = client.search(
                body=query,
                index=index)
            num_found = response["hits"]["total"]["value"]
            if int(num_found)>0:
                doc = response["hits"]["hits"][0]["_source"]
                h = md5()
                h.update(json.dumps(doc).encode('utf-8')) 
                new_hash = h.hexdigest() 
            else:
                 continue

            # print(response)
            # print(response["hits"]["total"]["value"])
            # print(response["hits"]["hits"][0]["_source"])

            if(hash.decode() == new_hash):
                matched.append(id)
            else:
                mismatched.append(id)   

    except Exception as e:
            print(e) 

    finally:
            client.close()

    return (matched, mismatched)

def get_solr_keys(r,parser):
    res = []
    if r is None:
        r = Redis.from_url(url=parser.get("redis", "url"))
    keys = r.keys()
    return keys

def process_id(op_type, index,docs):
    actions = []
    for doc in docs:
        # print(doc)
        # print("index",index)
        actions.append({"_op_type": op_type, "_index": index, "_id":doc["documentId"], "_source": doc})
    return actions
def process_copy(op_type, index,docs):
    actions = []
    for doc in docs:
        actions.append({"_op_type": op_type, "_index": index, "_source": doc})
    return actions

def getAllCores(coreadmin_url):
    admin = sa.SolrCoreAdmin(url=coreadmin_url)
    return admin.get_all_cores()


if __name__ == "__main__":

    main()
    # getAllJobruns()
    # print(get_solr_keys())
    # os.saveResult("solr_host", "solr_core","host" ,"index","rc_solr", "rc_search_solr","os_indexed_rc_count", "mr_succeeded", "mr_failed","query"+"&"+"queryString","time_taken")
    # print(getAllCores("http://lunr-sandbox.cernerasp.com:8080/CERNABCN_solr/"))

    # gallery_items()
   

def test():
    parser = RawConfigParser()
    parser.read('query_config.properties')
    host = parser.get("opensearch", "host")
    port = parser.get("opensearch", "port")
    user = parser.get("opensearch", "user")
    password = parser.get("opensearch", "password")
    client =os.connect(host,port, user,password)
    index =  parser.get("opensearch", "index")
    docs_cnt = validateOpensearchIndex(client,index)
    print(docs_cnt)
    # admin = sa.SolrCoreAdmin(url=constr)
    # re = admin.get_all_cores(name=solr_core)
    # rc_solr = re.get(solr_core)
    # rc_search_solr = len(result)
    # print(rc_solr)
    # print(rc_search_solr)
    # code to delete opensearch index 

    # parser = RawConfigParser()
    # parser.read('query_config.properties')
    # host = parser.get("opensearch", "host")
    # port = parser.get("opensearch", "port")
    # user = parser.get("opensearch", "user")
    # password = parser.get("opensearch", "password")

    # client = os.connect(host,port,user,password)
    # os.deleteindex(client,"test-index")
    
    # query = {
    # 'size': 5,
    # 'query': {
    #     'match': {
    #     "id":"*"
    #     }
    # }
    # }
    # response = client.search(body=query,index="test-index")
    # print(response)
    # res = os.deleteindex(client,"test-index")
    # print(res)

    # to get core and numof docs in core
    # coreadmin_url = 'http://129.213.114.157:8983/solr/'
    # admin = solrcoreadmin.SolrCoreAdmin(url=coreadmin_url)
    # cores = admin.get_all_cores()
    # print(cores)

    pass
