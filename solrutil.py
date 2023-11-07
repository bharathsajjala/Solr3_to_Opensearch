import pysolr

import urllib
import json
import solrcoreadmin as sa


def connect(host,auth,path, port,core):
    try:
        con = None
        constr ="http://"+host+":"+port+"/"+path+"/"
        constr = constr+core+"/" if len(core)>0 else constr 
        print(constr)
        con = pysolr.Solr(constr,auth=auth, always_commit=True, timeout=10)
    except :
        raise Exception("Solr connection error ")

    return con

def ping(con):

    return con.ping()


    return 

def list_cores(url,name=None):

        admin = sa.SolrCoreAdmin(url=url)
        cores = admin.list_cores()
        print(cores)
        return cores
         

            
def getrecordCount(con, core):

    return 


