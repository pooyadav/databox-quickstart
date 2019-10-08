__author__ = "Poonam Yadav"
__copyright__ = "Copyright 2007, The Databox Project"
__email__ = "p.yadav@acm.org"

import pythonzestclient as zestClient
import lib.config as config
import lib.arbiter_client as arbiterClient
import urllib3
import os
import json
from flask import Flask
import ssl
from io import StringIO
import base64


# StoreClient returns a new client to read and write data to stores
# storeEndpoint is provided in the DATABOX_ZMQ_ENDPOINT environment variable
# and arbiterEndpoint is provided by the DATABOX_ARBITER_ENDPOINT environment variable
#to databox apps and drivers.

class StoreClient:
    def __init__(self, config,storeEndpoint, arbiterEndpoint, storetype, enableLogging):
        self.config = config
        self.zestEndpoint = storeEndpoint
        self.zestDealerEndpoint = storeEndpoint.replace(":5555", ":5556")
        self.zestCli = zestClient.PyZestClient(config.CORE_STORE_KEY, self.zestEndpoint,self.zestDealerEndpoint,  enableLogging)
        self.arbiterCli = arbiterClient.new_arbiter_client(arbiterEndpoint, enableLogging)
        self.storetype = storetype

    def RegisterDatasource(self, DataSourceMetadata):
        return _registerDatasource(self.arbiterCli, self.zestCli, DataSourceMetadata)

    def GetDatasourceCatalogue(self):
        return _read(self.arbiterCli, self.zestCli, '/cat', '/cat', 'JSON')

    def getStoreUrlFromHypercat(self, hypercat):
        dsm = HypercatToSourceDataMetadata(hypercat)
        u =  urllib3.util.parse_url(hypercatObj.href)
        return u.scheme + '//' + u.host

    def read(self, dataSourceID, key, contentFormat = 'JSON'):
        if(self.storetype == 'KV'):
            path = "/kv/" + dataSourceID + "/" + key
            return _read(self.arbiterCli, self.zestCli, path, path, contentFormat)

    def latest(self, dataSourceID):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "latest"
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/" + "latest"
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def earliest(self, dataSourceID):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "earliest"
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/" + "earliest"
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def lastN(self, dataSourceID, n):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "last" + "/" + str(n)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/"+ "last" + "/" + str(n)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def firstN(self, dataSourceID, n):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "first" + "/" + str(n)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/" + "first" + "/" + str(n)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def since(self, dataSourceID, sinceTimeStamp):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "since" + "/" + str(sinceTimeStamp)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/" + "since" + "/" + str(sinceTimeStamp)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def range(self, dataSourceID, formTimeStamp, toTimeStamp):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID + "/" + "range" + "/" + str(formTimeStamp) + "/" + str(toTimeStamp)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID + "/" + "range" + "/" + str(formTimeStamp) + "/" + str(toTimeStamp)
            return _read(self.arbiterCli, self.zestCli, path, path, 'JSON')

    def observe(self, dataSourceID, timeOut = 0):
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID
            return _observe(self.arbiterCli, self.zestCli, path, timeOut, 'JSON')
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID
            return _observe(self.arbiterCli, self.zestCli, path, timeOut, 'JSON')

    def stopObserving(self, dataSourceID):
        print("Work-in-progress")



    def write(self, dataSourceID, payload, **kwargs):
        if("key" in kwargs):
            key = kwargs['key']
        if("contentFormat" in kwargs):
            contentFormat = kwargs['contentFormat']
        if (self.storetype == 'KV'):
            path = "/kv/" + dataSourceID + "/" + key
            return _write(self.arbiterCli, self.zestCli, path, path, payload, contentFormat)
        if (self.storetype == 'TS'):
            path = "/ts/" + dataSourceID
            return _write(self.arbiterCli, self.zestCli, path, path, payload, contentFormat)
        if (self.storetype == 'TSB'):
            path = "/ts/blob/" + dataSourceID
            return _write(self.arbiterCli, self.zestCli, path, path, payload, contentFormat)

    def writeAt(self, dataSourceID, timestamp, payload):
        path = "/ts/blob/" + dataSourceID + '/at/'+ str(timestamp)
        tokenPath = "/ts/blob/" + dataSourceID + '/at/*'
        return _write(self.arbiterCli, self.zestCli, path, tokenPath, payload, 'JSON')



def _registerDatasource(arbiterClient, zestClient, DataSourceMetadata):
    if ValidateDataSourceMetadata(DataSourceMetadata) == True:
        try:
            hyperCatObj = DataSourceMetadataToHypercat(zestClient.endpoint + '/' + DataSourceMetadata['StoreType'] + '/', DataSourceMetadata)
            hyperCatString = json.dumps(hyperCatObj)
            _write(arbiterClient, zestClient, '/cat', '/cat', hyperCatString, 'JSON')
        except ValueError:
            print("Error:: RegisterDatasource Error "+ ValueError)
    else:
        print("Error:: InValid DataSourceMetaData ")

def DataSourceMetadataToHypercat(endpoint, metadata):
    ValidateDataSourceMetadata(metadata)
    cat = {'item-metadata': [{"rel": "urn:X-hypercat:rels:hasDescription:en",
                              "val": metadata['Description']},
                             {"rel": "urn:X-hypercat:rels:isContentType",
                              "val": metadata['ContentType']},
                             {"rel": "urn:X-databox:rels:hasVendor",
                              "val": metadata['Vendor']},
                             {"rel": "urn:X-databox:rels:hasType",
                              "val": metadata['DataSourceType']},
                             {"rel": "urn:X-databox:rels:hasDatasourceid",
                              "val": metadata['DataSourceID']},
                             {"rel": "urn:X-databox:rels:hasStoreType",
                              "val": metadata['StoreType']}],
           'href': endpoint + metadata['DataSourceID']

           }

    if(metadata['IsActuator']):
        cat['item-metadata'].append({"rel":"urn:X-databox:rels:isActuator", "val":metadata['IsActuator']})

    if(metadata['Unit']):
        cat['item-metadata'].append({"rel":"urn:X-databox:rels:hasUnit","val":metadata['Unit']})

    if (metadata['Location']):
        cat['item-metadata'].append({"rel":"urn:X-databox:rels:hasLocation","val":metadata['Location']})

    return cat




def ValidateDataSourceMetadata(DataSourceMetadata):
    try:
        if not DataSourceMetadata or not DataSourceMetadata['Description'] or not DataSourceMetadata['ContentType'] or not DataSourceMetadata['Vendor'] or not DataSourceMetadata['DataSourceType'] or not DataSourceMetadata['DataSourceID'] or not DataSourceMetadata['StoreType']:
            raise ValueError
        else:
            checkStoreType(DataSourceMetadata['StoreType'])
    except ValueError:
        print("Error:: Not a valid DataSourceMetadata object missing required property")
        return False

    return True

def checkStoreType(StoreType):
    try:
        switcher = {"kv": True, "ts": True, "ts/blob": True}
        if switcher.get(StoreType) == True:
            return True
        else:
            raise ValueError
    except ValueError:
        print("Error:: DataSourceMetadata invalid StoreType can be kv,ts or ts/blob")


def newKeyValueClient(storeEndpoint, arbiterEndpoint, enableLogging): #this could be kv or blob
        return  StoreClient(config,storeEndpoint, arbiterEndpoint, 'KV', enableLogging)

def newTimeSeriesBlobClient(storeEndpoint, arbiterEndpoint, enableLogging):
        return StoreClient(config, storeEndpoint, arbiterEndpoint,'TSB', enableLogging)


def newTimeSeriesClient(storeEndpoint, arbiterEndpoint, enableLogging):
    return StoreClient(config, storeEndpoint, arbiterEndpoint, 'TS', enableLogging)




def _read(arbiterClient, zestClient, path, tokenPath, contentFormat = 'JSON'):
    validateContentFormat(contentFormat)
    try:
        tokenString = ""
        endPoint = urllib3.util.parse_url(zestClient.endpoint)
        tokenString = arbiterClient.requestToken(endPoint.host, tokenPath, "GET")
        response = zestClient.get(path, contentFormat, tokenString)
        if(response is not None and contentFormat == 'JSON'):
            response = json.loads(response)
        return response

    except ValueError:
        print("Read Error: for path " + ValueError)



def _write(arbiterClient, zestClient, path, tokenPath, payload, contentFormat = 'JSON'):
    validateContentFormat(contentFormat)
    if(payload is not None and  type(payload )=='object'  and  contentFormat == 'JSON'):
        try:
            payload = json.dumps(payload)
        except ValueError:
            print("Write Error: invalid json payload " + ValueError)

    try:
        endPoint = urllib3.util.parse_url(zestClient.endpoint)
        token = arbiterClient.requestToken(endPoint.host, tokenPath, "POST", [])
        if (token is None):
            #token = arbiter_token
            token = config.ARBITER_TOKEN#this is just for testing
        payload = str(payload)
        response = zestClient.post(path, payload, contentFormat, token)

        #Todo this is here to maintain backward compatibility after moving to zest should be removed
        if(response == ''):
            response = 'created'
        return response
    except ValueError:
        print("Write Error: for path " + ValueError)


def _observe(arbiterClient, zestClient, path, timeOut, contentFormat = 'JSON'):
    print("work-in-progress")



def validateContentFormat(contentFormat):
    try:
        switcher = {"TEXT": True, "BINARY": True, "JSON": True}
        if switcher.get(contentFormat.upper()) == True:
            return True
        else:
            raise ValueError
    except ValueError:
        print("Error: Unsupported content format")




def __del__(self):
    self.store.closeSockets()


def  newDataSourceMetadata():
     return {'Description': ' ',
             'ContentType': ' ',
             'Vendor': ' ',
             'DataSourceType': ' ',
             'DataSourceID': ' ',
             'StoreType': ' ',
             'IsActuator': False,
             'Unit': ' ',
             'Location': ' ',}

#hostname = os.environ['DATABOX_LOCAL_NAME'] or socket.getfqdn()
