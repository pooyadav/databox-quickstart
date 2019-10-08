__author__ = "Poonam Yadav"
__copyright__ = "Copyright 2017, The Databox Project"
__credits__ = ["Databox team"]
__license__ = "GPL"
__version__ = "0.0.1"
__maintainer__ = "Poonam Yadav"
__email__ = "p.yadav@acm.org"
__status__ = "Development"

#This code is setup for testing python library outside databox. Inside Databox, STORE_URI Will be extrated from env DATABOX_ZMQ_ENDPOINT.
# ARBITER URI will be drived from that as well (todo)

import sys
from flask import Flask
import ssl
import os
import time

sys.path.insert(1, '../')
from lib import core_store as databox #main function as providing the storeclient of core store.
from lib import config as config
import datetime as datetime

TEST_STORE_URI =  os.environ.get('DATABOX_ZMQ_ENDPOINT') or "tcp://127.0.0.1:5555"
TEST_ARBITER_URI = os.environ.get('DATABOX_ARBITER_ENDPOINT') or "tcp://127.0.0.1:4444"
DATA_SOURCE_ID = str(datetime.date.today())

#newKVStore = databox.newKeyValueClient(TEST_STORE_URI, TEST_ARBITER_URI, False)
#res = newKVStore.write("testdata1", 'KeyWrite', '{\"TEST\": \"data\"}', 'JSON')
#res = newKVStore.read("testdata1", 'KeyWrite','JSON')
#print("Read data from store " + str(res))

newTSStore = databox.newTimeSeriesBlobClient(TEST_STORE_URI, TEST_ARBITER_URI, False)
timeline = databox.newDataSourceMetadata()
timeline['Description'] = 'Twitter user timeline data'
timeline['ContentType'] = 'application/json'
timeline['Vendor'] = 'Databox Inc.'
timeline['DataSourceType'] = 'testdata1'
timeline['DataSourceID'] = 'testdata1'
timeline['StoreType'] = 'ts'

try:
    newTSStore.RegisterDatasource(timeline)
except ValueError:
    print("error in registoring datastore")
cat = newTSStore.GetDatasourceCatalogue()

res = newTSStore.write('testdata1','{\"idx\": \"16\"}',  contentFormat ='JSON')

res1 = newTSStore.latest('testdata1')
if(res1):
    print("Data res1 latest from the store " + str(res1))

res2 = newTSStore.earliest('testdata1')
if(res2):
    print("Data  res2  earliest from the store " + str(res2))

res = newTSStore.write('testdata1','{\"idx\": \"17\"}',  contentFormat ='JSON')

res3 = newTSStore.lastN('testdata1', 1)
if(res3):
    print("Data res3 last 1 from the store " + str(res3))

res4 = newTSStore.lastN('testdata1', 2)
if(res4):
    print("Data res4 last 2 from the store " + str(res4))

res5 = newTSStore.since('testdata1', 1570575084924)
if(res5):
    print("Data res5 since the time<1570575084924> from the store " + str(res5))


res6 = newTSStore.range('testdata1', 1570575084924, 1570575441326)
if(res6):
    print("Data res6 in range<1570575084924, 1570575441326> from the store " + str(res6))


res7 = newTSStore.writeAt('testdata1',1570575084925,'{\"idx\": \"20\"}')

res8 = newTSStore.latest('testdata1')

if(res8):
    print("Data res8 lastest from the store " + str(res8))


#app = Flask(__name__)
#credentials  = config.getHttpsCredentials()
#fp_cert = open(os.path.abspath("certnew.pem"), "w+")
#fp_cert.write(str(credentials['cert']))
#fp_cert.close()

#fp_key = open(os.path.abspath("keynew.pem"), "w+")
#fp_key.write(str(credentials['key']))
#fp_key.close()

#ctx = ('certnew.pem', 'keynew.pem')

#@app.route("/ui")
#def hello():
#   return "Hello World!"

#if __name__ == "__main__":
#     print("A Databox Driver")
     #time.sleep(500)
     #app.run(host='0.0.0.0', port=8080, ssl_context=ctx)

