from opensearchpy import OpenSearch
from opensearchpy.helpers import bulk,parallel_bulk
import traceback
import pandas as pd
import append_df_to_excel as xl
import os


def connect(host,port,user, password):

    host_obj = [{'host': host, 'port':port} ]
    auth = (user, password)
    client = OpenSearch(
        hosts=host,
        http_compress=True,
        http_auth=auth,
        use_ssl=True,
        verify_certs=False,
        ssl_assert_hostname=host,
        ssl_show_warn=True)

    return client

def bulkupload(client, actions):

     return  bulk(client, actions)

def search(client, index,q ):

    query = {
        'size': 5,
        'query': {
            'multi_match': {
                'query': q
            }
        }
    }
    response = client.search(body=query,index= index)
    return response


def deleteindex(client, index_name):

    response = client.indices.delete(index=index_name)
 
    print('\nDeleting index:')
    print(response)
    
    return 
def append_df_to_excel(df, excel_path ):
    df_excel = pd.read_excel(excel_path)
    result = pd.concat([df_excel, df], ignore_index=True)
    result.to_excel(excel_path, index=False)


def saveResult(host, core, os_host, os_index,rc_solr, rc_search_solr,os_indexed_rc_count,mr_success, mr_failed,params,run_time):

    try:
        data = {"solr_host":host,"solr_core":core, "solr_recordcount":rc_solr,
        "solrsearchresultcount":rc_search_solr,"os_host":os_host,"os_index":os_index,
         "os_indexed_count":os_indexed_rc_count, "os_mr_success":mr_success, "os_mr_failed": str(mr_failed),"solr_query":str(params),"tool_runtime":run_time}
        df = pd.DataFrame(data=data, index=[1])
        append_df_to_excel(df, os.path.abspath("./job_result.xlsx"))
        print("Migrated ",str(mr_success)," Solr docs to OpenSearch ")
        print("saved job result:")
    except  Exception as e:
        print("An exception occurred:", e)
        traceback.format_exc()
        

    return 
