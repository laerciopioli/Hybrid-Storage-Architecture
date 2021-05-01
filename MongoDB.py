# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''


import base64
import bson
from bson.binary import Binary
from bson.objectid import ObjectId
import gridfs
from gridfs.grid_file import (GridIn,
                              GridOut,
                              GridOutCursor,
                              DEFAULT_CHUNK_SIZE)
import io
import math
import os
from pprint import pprint
from pymongo import MongoClient
import timeit

from Sensor import SensorIoT


client = MongoClient('localhost', 27017)

database = client['test']
collection = database['escalares_posicionais']

# V�rias temperatures
def mongodb_temperatures_unic_recording(file_name):
    
    with io.open(file_name, 'rb', buffering=2<<16) as file:
        index = 0
        t = 0
        num_str = ""
        print '>>> Start recording temperatures on mongodb <<<'
        for line in file:
            line_str = ""
            line_str = str(line)
            while (index < len(line_str)):
                if (line[index] != ','):
                    num_str = num_str + line[index]
                    index += 1
                else:
                    num = int(num_str)
                    begin = timeit.default_timer()
                    collection.insert({'_id':collection.count() + 1 ,'sensor':'DHT11', 'temperature' : num})
                    end = timeit.default_timer()
                    t = t + (begin - end)/1000000
                    print 'mongodb partial length of recording: %f seconds' % t
                    index += 1
                    num_str = ""   
        print '>>> end of recording temperatures on mongodb <<<'
        t = t/60
        print 'Mongodb estimated recording time: %f minutes' % t


def mongodb_temperatures_scalar_recording(dataset_size):
        t = 0
        cont = 1
        temperatures = SensorIoT.gerar_dados_escalares(dataset_size)
        
        for temperature in temperatures:
            begin = timeit.default_timer()
            collection.insert({'_id':cont, 'temperature' : temperature})
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            time = abs(t) * 1000000
            cont += 1
        return time

def mongodb_multimedia_recording(local, file):
    
    file_and_path = local + "/" + file

    chunk_size = 1024
    
    gridfs.grid_file.DEFAULT_CHUNK_SIZE = chunk_size

    fs = gridfs.GridFS(database)

    with open(file_and_path, 'rb') as a:
        f = a.read()
        encoded = Binary(f, 0)

    
    begin = timeit.default_timer()
    id = fs.put(encoded, filename = file)

    end = timeit.default_timer()
    t = (begin - end)/1000000
    t = abs(t)
    time = abs(t)*1000000
    
    id_time = []
    id_time.append(id)
    id_time.append(time)

    return id_time

def del_mongodb_multimedia_unic(id):
    fs = gridfs.GridFS(database)
    begin = timeit.default_timer()
    if fs.exists({"_id": id}):
        end = timeit.default_timer()
        t = (begin - end)/1000000
        time = abs(t) * 1000
        print 'file located in %d miliseconds. ' % time
        fs.delete(id)
    else:
        print 'file does not found!'

def multimedia_pieces_insertion(path):
    directory = os.listdir(path)
    for file in directory:
        file_and_path = path + "/" + file
        with open(file_and_path, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            collection.insert({'file_name' : file, 'file': encoded})

#https://stackoverflow.com/questions/31155508/how-to-store-and-get-binary-files-from-gridfs
def mongodb_multimedia_chunk_query(Idfile):
    begin = timeit.default_timer()
    database.fs.chunks.find_one({"files_id" : ObjectId(Idfile), "n" : (database.fs.chunks.count() - 1)})
    end = timeit.default_timer()
    t = (begin - end)/1000000
    t = abs(t)
    time = abs(t)*1000000
    return time

    
def mongodb_multimedia_query(file):
    if (collection.find({'file_name' : file}).count() > 0):     
        query = collection.find({'file_name' : file}).explain()['executionStats']
        for atrib, value in query.items():
            if atrib == "executionTimeMillis":
                print 'Search time: %.20f' % (float(value) / 1000) + ' seconds.'
    else:
        print "Item not foundo"
    
def mongodb_temperature_query(temperature):
    if (collection.find({'temperature' : temperature}).count() > 0):     
        query = collection.find({'temperature' : temperature}).explain()['executionStats']
        for atrib, value in query.items():
            if atrib == "executionTimeMillis":
                return float(value) / 1000

            
def mongodb_position_data_recording(dataset_size):
        t = 0
        cont = 1
        latitudes = SensorIoT.gerar_dados_posicionais(dataset_size)
        for latitude in latitudes:
            begin = timeit.default_timer()
            collection.insert({'_id': cont , 'latitude' : latitude})
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            time = abs(t) * 1000000
            cont += 1
        return time

def mongodb_position_query(latitude):
    if (collection.find({'latitude' : latitude}).count() > 0):     
        query = collection.find({'latitude' : latitude}).explain()['executionStats']
        for atrib, value in query.items():
            if atrib == "executionTimeMillis":
                return float(value) / 1000

def mongodb_general_scalar_data_record(dataset, generalCount):
    mongodb_data_cleaning()
    text = ""
    scalarWritingCount = 0
    scalarTime = 0
    print 'Mongodb scalar data recording in %dMb.' % (dataset/1048576)
    while scalarWritingCount < generalCount:
        scalarTime = scalarTime + mongodb_temperatures_scalar_recording(dataset)
        scalarWritingCount = scalarWritingCount + 1
        if scalarWritingCount < generalCount:
            collection.remove({})
    text = text +  'Scalar data recording in %dMb with average time of %d seconds.' % (dataset/1048576, scalarTime/scalarWritingCount) + '\n'
    return text

def mongodb_general_positional_data_record(dataset, generalCount):
    mongodb_data_cleaning()
    text = ""
    collection.remove({})
    positionalWritingCount = 0
    positionaltime = 0

    print 'Mongodb positional data Writing %dMb' % (dataset/1048576)
    while positionalWritingCount < generalCount:
        positionaltime = positionaltime + mongodb_position_data_recording(dataset)
        positionalWritingCount = positionalWritingCount + 1
        if positionalWritingCount < generalCount:
            collection.remove({})
    text = text + 'Positional data recording in %dMb with average time of %d seconds.' % (dataset/1048576, positionaltime/positionalWritingCount) + '\n'
    return text

def mongodb_general_scalar_data_record(dataset, generalCount):
    mongodb_data_cleaning()
    text = ""
    multimediaWritingCount = 0
    timeescritamultimidia = 0
    print 'Mongodb multimedia data Writing %dMb' % (dataset/1048576)
    while multimediaWritingCount < generalCount:
        id_time = mongodb_multimedia_recording("directory", str(dataset) + ".mp4")
        timeescritamultimidia = timeescritamultimidia + id_time[1]
        multimediaWritingCount = multimediaWritingCount  + 1
    text = text + 'Multimedia data recording in %dMb with average time of %d seconds.' % (dataset/1048576, timeescritamultimidia/multimediaWritingCount) + '\n'
    return text

def mongodb_general_scalar_data_query(dataset, generalCount):
    text = ""
    contadorqueryescalar = 0
    timequeryescalar = 0
    collection.insert({'temperature' : 100})
    print 'MongoDb scalar data query in %dMb.' % (dataset/1048576)
    while contadorqueryescalar < generalCount:
        timequeryescalar = timequeryescalar + mongodb_temperature_query(100)
        contadorqueryescalar = contadorqueryescalar + 1
    text = text + 'query of key number 100 in %dMb of scalar data with average final time of %.6f seconds.' % (dataset/1048576, timequeryescalar/contadorqueryescalar) + '\n'
    text = text +  '--------------------------------------------------------------------------------------------------------------------------------\n'
    return text

def mongodb_general_positional_data_query(dataset, generalCount):
    text = ""
    #Inicializador contador de queryposicional
    contadorqueryposicional = 0
    #Inicializador dos times de query posicional
    timequeryposicional = 0
    collection.insert({'latitude' : '0° 0´N'})
    print 'MongoDb positional data query in %dMb.' % (dataset/1048576)
    while contadorqueryposicional < generalCount:
        timequeryposicional = timequeryposicional + mongodb_position_query('0° 0´N')
        contadorqueryposicional = contadorqueryposicional + 1
    text = text +  'query of key number 0° 0´N em %dMb of positional data with average final time of %.6f seconds.' % (dataset/1048576, timequeryposicional/contadorqueryposicional) + '\n'
    text = text +  '--------------------------------------------------------------------------------------------------------------------------------\n'
    return text

def mongodb_general_multimedia_data_query(dataset, generalCount):
    text = ""
    contadorquerymultimidia = 0
    timequerymultimidia = 0
    database.fs.chunks.drop()
    database.fs.files.drop()
    id_time = mongodb_multimedia_recording("directory", str(dataset) + ".mp4")
    Idfile = id_time[0]
    print 'MongoDb multimedia data query in %dMb.' % (dataset/1048576)
    while contadorquerymultimidia < generalCount:
        timequerymultimidia = timequerymultimidia + mongodb_multimedia_chunk_query(Idfile)
        contadorquerymultimidia = contadorquerymultimidia + 1
    text = text + 'Multimedia data query in %dMb of data with average final time of %.6f seconds.' % (dataset/1048576, timequerymultimidia/contadorquerymultimidia) + '\n'
    text = text + '--------------------------------------------------------------------------------------------------------------------------------\n'
    return text

#################################
###  GENERAL EXEC SCRIPT      ###
#################################


def mongodb_data_cleaning():
    collection.remove({})
    database.fs.chunks.drop()
    database.fs.files.drop()

def run_mongodb_writing_and_read():
    t_geral = 0 
    message = ""
    
    general_begin = timeit.default_timer()
    
    mongodb_data_cleaning()
        
    datasets = [2097152]

    generalCount = 10

    for dataset in datasets:
        
        
        #message = message +  mongodb_general_scalar_data_record(dataset, generalCount)
        
        #message = message + mongodb_general_scalar_data_query(dataset, generalCount)
        
        #message = message + mongodb_general_positional_data_record(dataset, generalCount)
        
        #message = message + mongodb_general_positional_data_query(dataset, generalCount)
        
        message = message + mongodb_general_scalar_data_record(dataset, generalCount)
        
        #message = message + mongodb_general_multimedia_data_query(dataset, generalCount)
                

    end_geral = timeit.default_timer()
    t_geral = t_geral + (general_begin - end_geral)/1000000
    time_geral = abs(t_geral) * 1000000
    
    message = message + 'Total experiment time: %d seconds.\n' % time_geral
        

    file_name = "execucao_geral_mongodb.txt"
    if (os.path.exists(file_name)):
        os.remove(file_name)
        
    file = open(file_name,'wb+')
    file.write(message)
    file.close()
        
#################################
###  GENERAL EXEC SCRIPT      ###
#################################
run_mongodb_writing_and_read()

'''database.fs.chunks.drop()
database.fs.files.drop()
id_time = mongodb_multimedia_recording("directory", str(67108864) + ".mp4")
Idfile = id_time[0]
print Idfile
mongodb_multimedia_chunk_query('5b523a505236ae28753d0c5b')'''