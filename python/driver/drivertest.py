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
from lib import core_store as store #main function as providing the storeclient of core store.
from lib import config as config
import datetime as datetime

TEST_STORE_URI =  os.environ.get('DATABOX_ZMQ_ENDPOINT') or "tcp://127.0.0.1:5555"
TEST_ARBITER_URI = os.environ.get('DATABOX_ARBITER_ENDPOINT') or "tcp://127.0.0.1:4444"
DATA_SOURCE_ID = str(datetime.date.today())

newStore = store.StoreClient.NewStoreClient(TEST_STORE_URI, TEST_ARBITER_URI, False)
res = newStore.storeCli.write("testdata1", 'KeyWrite', '{\"TEST\": \"data\"}', 'JSON')
res = newStore.storeCli.read("testdata1", 'KeyWrite','JSON')
print("Read data from store " + str(res))


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

