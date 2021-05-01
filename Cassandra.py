# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''

from cassandra.cluster import Cluster
from Sensor import SensorIoT
import timeit
import json
import os
from bson.binary import Binary

# Conection
cluster = Cluster()
# Conect to the keyspace 'dadossensoriot'
session = cluster.connect('dadossensoriot')

# Seting operations timeout to 5h
session.default_timeout = 18000
session.default_fetch_size = 50

def escalar_data_record_cassandra(dataset_size):
        t = 0
        
        #print '>>> Beginning temperature in-memory generation <<<'
        temperatures = SensorIoT.gerar_dados_escalares(dataset_size)
        #print '>>> Finishing temperature in-memory generation <<<'
        
        cont = 0
        
        for temperature in temperatures:
            start= timeit.default_timer()
            
            # Cassandra Insertion            
            session.execute("insert into temperatures disk (id, temperature) values (%s, %s)", (cont, temperature))
            
            end= timeit.default_timer()
            t = t + (start- end)/1000000
            time = abs(t) * 1000000
            cont += 1
            
        return time
   
def positional_data_record_cassandra(dataset_size):
        t = 0
        #print '>>> Beginning latitude in-memory generation <<<'
        latitudes = SensorIoT.gerar_dados_posicionais(dataset_size)
        #print '>>> Finishing latitude in-memory generation <<<'
        
        cont = 0
        
        for latitude in latitudes:
            start= timeit.default_timer()
            
            #Cassandra Insertion
            session.execute("insert into latitudesdisk (id, latitude) values (%s, %s)", (cont, latitude))
            
            end = timeit.default_timer()
            t = t + (start- end)/1000000
            time = abs(t) * 1000000
            cont += 1
            
        return time
        

def multimedia_data_record_cassandra(local, file):
    
    place_and_file = local + "/" + file
    totaltime = 0
    #chunk_size = tamanho
    chunk_size = 1024
    with open(place_and_file, 'rb') as infile:
        data = infile.read()
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        encoded = str(json.dumps(chunk, ensure_ascii=False, encoding='utf-8'))
        
        params = [i, bytearray(encoded), file, i]
        start = timeit.default_timer()
        session.execute("INSERT INTO multimediadisk (id, file, filename, splitno) VALUES (%s, %s, %s, %s)", params)
        end = timeit.default_timer()
        t = (start - end)/1000000
        time= abs(t) * 1000000
        totaltime += time
        start = 0
        end = 0 

    return totaltime
    
def inserir_pedacos_multimidia(path):
    path = os.listdir(path)
    cont = 0
    for file in path:
        place_and_file = path + "/" + file
        with open(place_and_file, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            params = [cont, bytearray(encoded), file, 0]
            session.execute("INSERT INTO multimediadisk (id, file, filename, splitno) VALUES (%s, %s, %s, %s)", params)
            cont += 1      

def search_multimedia_data_cassandra(file, max_chunk):
    query = "select count(*) from multimediadisk where filename = "  + "'" + str(file) + "'" + " and splitno = " + str(max_chunk) + " allow filtering;"
    start = timeit.default_timer()
    con = session.execute(query)
    end = timeit.default_timer()
    t = (start - end)/1000000
    time= abs(t) * 1000000
    if con[0].count > 0:
        return time
    

        
# query dado escalar ou posicional
def data_query_cassandra(chave):
    database = ""
    tipo = ""
    query = ""
    if '°' in chave:
        database = "latitudesdisk" 
        tipo = "latitude"
        query = "select count(*) from " + database + " where " + tipo + " = " + "'" + str(chave) + "'" + " ALLOW FILTERING"
    else:
        database = "temperaturesdisk" 
        tipo = "temperature"
        query = "select count(*) from " + database + " where " + tipo + " = " + chave + " ALLOW FILTERING"
    
    start = timeit.default_timer()
    con = session.execute(query)
    end = timeit.default_timer()
    t = (start - end)/1000000
    time= abs(t) * 1000000
    
    if con[0].count > 0:
        return time

def general_scalar_data_record_cassandra(dataset, general_count):
    text = ""
    #scalar writing count
    scalar_writing_count = 0
    #Initializer scalar writers count
    scalar_time = 0
    print 'Scalar data record %dMb in Cassandra.' % (dataset/1048576)
    while scalar_writing_count < general_count:
        print '%dª scalar writing run' % (scalar_writing_count+1)
        scalar_time = scalar_time + escalar_data_record_cassandra(dataset)
        scalar_writing_count = scalar_writing_count + 1
        #Não apaga a última execução para poder permitir as consultas
        if scalar_writing_count < general_count:
            session.execute('truncate table temperaturesdisk')
    text = text + 'Scalar data record of %dMb with average final time of %d seconds.' % (dataset/1048576, scalar_time/scalar_writing_count) + '\n'
    text = text + '---------------------------------------------------------------------------------------------------------------\n'
    print '---------------------------------------------------------------------------------------------------------------'
    return text

def general_positional_data_record_cassandra(dataset, general_count):
    text = ""
    #Positional writing count
    positional_writing_count = 0
    #Initializer positional writers count
    positional_time = 0
    print 'Positional data record of %dMb in Cassandra.' % (dataset/1048576)
    while positional_writing_count < general_count:
        print '%dª positional data run' % (positional_writing_count+1)
        positional_time = positional_time + positional_data_record_cassandra(dataset)
        positional_writing_count = positional_writing_count + 1
        if positional_writing_count < general_count:
            session.execute('truncate table latitudesdisk')
    text = text +  'Positional data record of %dMb with average final time of %d seconds.' % (dataset/1048576, positional_time/positional_writing_count) + '\n'
    text = text + '---------------------------------------------------------------------------------------------------------------\n'
    print '---------------------------------------------------------------------------------------------------------------'
    return text

def general_multimedia_data_record_cassandra(dataset, general_count):
    text = ""
    #Multimedia writings count
    multimedia_writing_count = 0
    #Initializer multimedia writers count
    multimedia_writing_time = 0
    print 'Multimídia data record of %dMb in Cassandra.' % (dataset/1048576)
    while multimedia_writing_count < general_count:
        print '%dª Multimídia write count' % (multimedia_writing_count+1)
        multimedia_writing_time = multimedia_writing_time + multimedia_data_record_cassandra("path", str(dataset) + ".mp4")
        multimedia_writing_count = multimedia_writing_count + 1
        if multimedia_writing_count < general_count:
            session.execute('truncate table multimediadisk')
    text = text + 'Multimedia data record %dMb with average final time of %d seconds.' % (dataset/1048576, multimedia_writing_time/multimedia_writing_count) + '\n'
    text = text + '----------------------------------------------------------------------------------------------------------------\n'
    return text

def general_scalar_data_query_cassandra(dataset, general_count):
    text = ""
    #Initializer query for scalar time
    scalar_query_time = 0
    #Scalar queries count
    scalar_query_count = 0
    #Scalar data query
    scalar_end = dataset/1024
    session.execute("insert into temperaturesdisk (id, temperature) values (%s, %s)", (scalar_end, 100))
    print 'query of scalar data %dMb in Cassandra.' % (dataset/1048576)
    while scalar_query_count < general_count:
        print '%dª scalar query count' % (scalar_query_count+1)
        scalar_query_time = scalar_query_time + data_query_cassandra("100")
        scalar_query_count = scalar_query_count + 1
    text = text + 'Query of the key 100 in %dMb of scalar data with average final time in %.6f seconds.' % (dataset/1048576, scalar_query_time/scalar_query_count) + '\n'
    text = text + '-----------------------------------------------------------------------------------------------------------------------------\n'
    print '-----------------------------------------------------------------------------------------------------------------------------'
    return text

def general_positional_data_query_cassandra(dataset, general_count):
    text = ""
    #Initializer query for positional time
    positional_query_time = 0
    #Positional queries count
    positional_query_count = 0
    #Positional data query
    positional_end = dataset/1024
    session.execute("insert into latitudesdisk(id, latitude) values (%s, %s)", (positional_end, "0° 0´N"))
    print 'Positional data query of  %dMb in Cassandra.' % (dataset/1048576)
    while positional_query_count < general_count:
        print '%dª positional query run' % (positional_query_count+1)
        positional_query_time = positional_query_time  + data_query_cassandra("0° 0´N")
        positional_query_count = positional_query_count + 1
    text = text +  'Query of the key 0° 0´N in %dMb of positional data with average of final time %.6f seconds.' % (dataset/1048576, positional_query_time/positional_query_count) + '\n'
    text = text + '-----------------------------------------------------------------------------------------------------------------------------------\n'
    print '-----------------------------------------------------------------------------------------------------------------------------------'
    return text

def general_multimedia_data_query_cassandra(dataset, general_count):
    text = ""
    #Initializer query for multimedia time
    multimedia_query_count = 0
    #Multimedia queries count
    multimedia_query_time = 0
    max_chunk = dataset/1024
    multimedia_data_record_cassandra("path", str(dataset) + ".mp4")
    print 'Multimedia data query %dMb in Cassandra.' % (dataset/1048576)
    while multimedia_query_count < general_count:
        print '%dª multimedia query run' % (multimedia_query_count+1)
        multimedia_query_time = multimedia_query_time + search_multimedia_data_cassandra(str(dataset)+".mp4", max_chunk)
        multimedia_query_count = multimedia_query_count + 1
    text = text + 'Query in %dMb of multimedia data with final average time %.6f seconds.' % (dataset/1048576, multimedia_query_time/multimedia_query_count) + '\n'
    text = text + '------------------------------------------------------------------------------------------------------------\n'
    print '------------------------------------------------------------------------------------------------------------'
    return text

def run_writing_reading_cassandra():
    #initial time
    t_general = 0 
    #experiment start
    general_begin = timeit.default_timer()
    datasets = [67108864]
    #General run counter
    general_count = 10
    #result file text
    message = ""
    
    #Data Cleaning
    session.execute('truncate table latitudesdisk')
    session.execute('truncate table multimediadisk')

    for dataset in datasets:
        #General scalar data query according to general_count
        message = message + general_scalar_data_query_cassandra(dataset, general_count)

    #Experimental final time
    general_end = timeit.default_timer()
    t_general = t_general + (general_begin - general_end)/1000000
    general_time = abs(t_general) * 1000000
    
    message = message + 'Total experiment time: %d seconds.\n' % general_time
    
    #Writing data file
    file_name = "execucao_geral_cassandra.txt"
    if (os.path.exists(file_name)):
        os.remove(file_name)
    
    file = open(file_name,'wb+')
    file.write(message)
    file.close()
    
#########################
### General Run SCRIPT###
#########################
# 1 - Configure and execute all datasets in memory (generate graphs and statistical variables)
# sudo dse-5.1.7/bin/dse cassandra -R
# 2 - CConfigure and execute all datasets on disk (generate graphs and statistical variables)
#########################
run_writing_reading_cassandra()
