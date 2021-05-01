# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''

import json
import re
import redis
from pymongo import MongoClient
from cassandra.cluster import Cluster

def main():

    #Exemplo JSON temperatura
    json_temperatura_string_setData = '''{
                                                    "operation" : "setData", 
                                                    "db" : "temperature_db", 
                                                    "data" : "56", 
                                                    "storageOption" : "w_performance", 
                                                    "dataAnalysis" : "spark"
                                                }'''
    
    json_temperatura_cassandra_string_setData = '''{
                                                    "operation" : "setData", 
                                                    "db" : "temperaturas", 
                                                    "data" : "56", 
                                                    "storageOption" : "cassandra", 
                                                    "dataAnalysis" : "spark"
                                                }'''
    
    #Exemplo JSON latitude
    json_latitude_string_setData = '''{
                                            "operation" : "setData", 
                                            "db" : "latitude_db", 
                                            "data" : "90° 53´N", 
                                            "storageOption" : "w_performance", 
                                            "dataAnalysis" : "spark"
                                        }'''

    #Multimedia example JSON
    json_multimedia_string_setData = '''{
                                                "operation" : "setData", 
                                                "db" : "multimedia_db", 
                                                "data" : "LaCasaDePapel01.mp4;BinData(0,'AAAAKGZ0eXBNNFYgAAAAAWlzb21hdmMxaXNvNk00QSBNNFYgbXA0MgAakaRtb292AAAAbG12aGQAAAAA1o8DytaPA8oAAAJYACXr5gABAAABAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAADAAAAFWlvZHMAAAAAEAcAT///KH//ABQYg3RyYWsAAABcdGtoZAAAAAHWjwPK1o8DzQAAAAEAAAAAACXroAAAAAAAAAAAAAAAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAEAAAAAAAAAAAAAAAAAAEAAAAADVQAAAeAAAAAAACRlZHRzAAAAHGVsc3QAAAAAAAAAAQAl66AAAAfQAAEAAAAUF/ttZGlhAAAAIG1kaGQAAAAA1o8DytaPA80AAGGoBiwEYFXEAAAAAABiaGRscgAAAAAAAAAAdmlkZQAAAAAAAAAAAAAAADI2NCN2aWRlbzpmcHM9MjU6cGFyPTE2MDoxNTlAR1BBQzAuNS4yLURFVi1yZXY5OTgtZ2JkZGEyZWUtbWFzdGVyAAAUF3FtaW5mAAAAFHZtaGQAAAABAAAAAAAAAAAAAAAkZGluZgAAABxkcmVmAAAAAAAAAAEAAAAMdXJsIAAAAAEAFBcxc3RibAAAANxzdHNkAAAAAAAAAAEAAADMYXZjMQAAAAAAAAABAAAAAAAAAAAAAAAAAAAAAANQAeAASAAAAEgAAAAAAAAAAQAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAABj//wAAABBwYXNwAAAAoAAAAJ8AAAA6YXZjQwFkAB7/4QAdZ2QAHqzZQNQ9v/AKAAnxAAADAAEAAAMAMo8WLZYBAAZo6+LLIsD9+PgAAAAAGHV1aWQAAAAAAAAAAAAAAAAAAAAAAAAAFGJ0cnQAAXvWAB2NaAAHBEAAAAAYc3R0cwAAAAAAAAABAAGUfAAAA+gADE9AY3R0cwAAAAAAAYnmAAAAAQAAB9AAAAABAAATiAAAAAEAAAfQAAAAAQAAAAAAAAABAAAD6AAAAAEAABOIAAAAAQAAB9AAAAABAAAAAAAAAAEAAAPoAAAAAQAAE4gAAAABAAAH0AAAAAEAAAAAAAAAAQAAA+gAAAABAAATiAAAAAEAAAfQAAAAAQAAAAAAAAABAAAD6AAAAAIAAAfQAAAAAQAAD6AAAAACAAAD6AAAAAEAABOIAAAAAQAAB9AAAAABAAAAAAAAAAEAAAPoAAAAAQAAE4gAAAABAAAH0AAAAAEAAAAAAAAAAQAAA+gAAAABAAATiAAAAAEAAAfQAA==')", 
                                                "storageOption" : "r_performance", 
                                                "dataAnalysis" : "spark"
                                              }'''


def getData(json_requisition):
    typ = IoTDataTypeIdentification(json_requisition)
    
    json_decodificado = json.loads(json_requisition)
    data= u''.join(json_decodificado['data']).encode('utf-8').strip()
    database = u''.join(json_decodificado['db']).encode('utf-8').strip()
    criteria = u''.join(json_decodificado['storageOption']).encode('utf-8').strip()
    name = ''
    if ( typ == 'multimedia' ):
        position = data.find(';')
        name = data[0:position]
        
    if ( (criteria == 'w_performance') or (typ != 'multimedia' and criteria == 'r_performance') ):
        print getDataRedis(data, database, typ)
    
    elif ( typ ==  'multimedia' and criteria == 'r_performance' ):
        getMongoDBData(name)
    else:
        getCassandraDBData(data, database)

#data recording
def setData(json_requisition):
    typ = IoTDataTypeIdentification(json_requisition)
    
    json_decodificado = json.loads(json_requisition)
    data= u''.join(json_decodificado['data']).encode('utf-8').strip()
    database = u''.join(json_decodificado['db']).encode('utf-8').strip()
    criteria = u''.join(json_decodificado['storageOption']).encode('utf-8').strip()
    name = ''
    if ( typ == 'multimedia' ):
        position = data.find(';')
        name = data[0:position]
    
    BDRecordingDefinition(typ, criteria, data, database, name)


def BDRecordingDefinition(typ, criteria, data, database, name):
    
    if ( (criteria == 'w_performance') or (typ != 'multimedia' and criteria == 'r_performance') ):
        redisRecord(data, database, typ)
    
    elif ( typ ==  'multimedia' and criteria == 'r_performance' ):
        mongoDBRecord(data, typ, name)
    else:
        cassandraRecord(data, database)


def redisRecord(data, database, typ):
    r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)
    r.set(database + '_' + typ + '_' + data, data)


def obterDadoRedis(data, database, typ):
    r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)
    return r.get(database + '_' + typ + '_' + data)


def gravarMongoDB(data, typ, name):
    client = MongoClient('localhost', 27017)
    bd = client['test']
    collection = bd['escalares_posicionais']
    collection.insert({'_id' : name, typ : data})


def getMongoDBData(name):
    client = MongoClient('localhost', 27017)
    bd = client['test']
    collection = bd['escalares_posicionais']
    cursor = collection.find({'_id' : name})
    for document in cursor:
        print document

def cassandraRecord(data, database):
    # Connection
    cluster = Cluster()
    # keyspace 'dadossensoriot' connection
    session = cluster.connect('dadossensoriot')
    session.execute("insert into " + database + "(id, temperatura) values (%s, %s)", (int(data), int(data)))
    
# Obter datado Cassandra
def getCassandraDBData(data, database):
    # Connection
    cluster = Cluster()
    # keyspace 'dadossensoriot' connection
    session = cluster.connect('dadossensoriot')
    query = "select temperatura from " + database + " where id = " + data
    line = session.execute(query)
    result = ""
    for l in line:
        result = str(l)
    result = result[ result.find("=")+1 : result.find(")") ]
    print result
    

def IoTDataTypeIdentification(json_requisition):
    json_decodificado = json.loads(json_requisition)
    data = u''.join(json_decodificado['data']).encode('utf-8').strip()
    
    #Casa os padrões para identificar o type
    pattern_scalar = re.compile('\d+[^W]')
    pattern_positional = re.compile("\d+°\s\d+´[W|N]")
    pattern_multimedia = re.compile(".*;BinData*")
    
    if (pattern_positional.match(data) is not None):
        #return 'Positional: %s' % data
        return 'positional'
    elif (pattern_scalar.match(data) is not None):
        #return 'Scalar: %s' % data
        return 'Scalar'
    elif (pattern_multimedia.match(data) is not None):
        #return 'Multimedia: %s' % data
        return 'multimedia'
    else:
        return 'type not found'

if __name__ == '__main__':
    main()