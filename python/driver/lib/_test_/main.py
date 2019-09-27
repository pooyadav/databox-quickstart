__author__ = "Poonam Yadav"
__copyright__ = "Copyright 2007, The Databox Project"
__email__ = "p.yadav@acm.org"

import urllib3
import os
import json
from flask import Flask
import ssl
import pythonzestclient
import unittest
import datetime as datetime

import sys

sys.path.insert(1, '../../')
#This file currently using only Zest, so only act as test for zest

zestEndpoint="tcp://0.0.0.0:5555"
zestDealerEndpoint="tcp://0.0.0.0:5556"

CORE_STORE_KEY="vl6wu0A@XP?}Or/&BR#LSxn>A+}L)p44/W[wXL3<"
tokenString="secret"

class WriteReadTestCase(unittest.TestCase):
    def setUp(self):
        self.zc = pythonzestclient.PyZestClient(CORE_STORE_KEY,zestEndpoint,zestDealerEndpoint)

    def testKVWrite(self):
        print('----------------------\n|       testKVWrite   |\n ----------------------')
        payLoad='{"TEST": "data"}'
        DATA_SOURCE_ID = str(datetime.date.today())
        key = 'KeyWrite'
        path = "/kv/" + DATA_SOURCE_ID + "/" + key
        contentFormat='JSON'
        print(path)
        print(payLoad)
        print(contentFormat)
        print(tokenString)
        response = self.zc.post(path, payLoad, contentFormat,tokenString)
        self.assertEqual(response, "")


app = Flask(__name__)

@app.route("/ui")
def hello():
    return "Hello World!"

if __name__ == "__main__":
     print("Nothing")
     app.run(host='127.0.0.1', port=8080)
     unittest.main()
