# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''


import redis
import timeit
import math
import json
import os
from bson.binary import Binary
from Sensor import SensorIoT

r = redis.StrictRedis(host = '127.0.0.1', port = 6379, db = 0)

# Consulta dado escalar ou positional
def redis_data_search(key):
    keys = r.keys('*')
    found = 0
    time = 0
    for key in keys:
        begin = timeit.default_timer()
        if key == key:
            found = 1
        end = timeit.default_timer()
        t = (begin - end)/1000000
        time = time + abs(t)*1000000
        if found == 1:
            return time
    
def redis_scalar_data_record(dataset_size):
        t = 0
        cont = 1
        temperatures = SensorIoT.gerar_dados_escalares(dataset_size)
        for temperature in temperatures:
                
                begin = timeit.default_timer()

                r.set(temperature, temperature)
            
                end = timeit.default_timer()
                t = t + (begin - end)/1000000

                cont += 1
                time = abs(t) * 1000000


        return int(time)                        

  
def redis_positional_data_record(dataset_size):
        t = 0
        cont = 1
        positionals = SensorIoT.gerar_dados_positionals(dataset_size)
        for positional in positionals:
            begin = timeit.default_timer()
            
            r.set(str(positional), str(positional))
            
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            cont += 1
            time = abs(t) * 1000000
            
        return int(time)
        
def redis_multimedia_record(local, file):
    
    local_e_file = local + "/" + file
    size = os.path.getsize(local_e_file)
    toraltime = 0
    i = 0
    chunk_size = 1024 
    with open(local_e_file, 'rb') as infile:
        data = infile.read()
    c = 0
    for i in range(0, len(data), chunk_size):
        chunk = data[i:i+chunk_size]
        encoded = str(json.dumps(chunk, ensure_ascii=False, encoding='utf-8'))
        
        begin = timeit.default_timer()
        
        r.set(c, encoded)
        
        end = timeit.default_timer()
        t = (begin - end)/1000000
        time = abs(t)
        toraltime += time
        begin = 0
        end = 0
        c += 1
        
    return abs(toraltime) * 1000000

def multimedia_pieces_insertion(path):
    directory = os.listdir(path)
    for file in directory:
        local_e_file = path + "/" + file
        with open(local_e_file, 'rb') as a:
            f = a.read()
            encoded = Binary(f, 0)
            r.set(file, encoded)            

def contar_registros():
    cont = 0
    keys = r.keys('*')
    for key in keys:
        cont += 1
    print cont

#################################
###  Exec GENERAL SCRIPT      ###
#################################
def run_redis_read_write():
    t_geral = 0
    
    general_begin = timeit.default_timer()

    
    r.flushall()
    
    message = ""
    
    datasets = [1048576, 2097152, 4194304, 8388608, 16777216, 33554432, 67108864]
    
    generalcount = 10

    for dataset in datasets:
    
        writercount = 0
    
        scalartime = 0
        timepositional = 0
    
        print 'Redis Scalar and positional data recording %dMb.' % (dataset/1048576)
        while writercount < generalcount:
            scalartime = scalartime + redis_scalar_data_record(dataset)
            timepositional = timepositional + redis_positional_data_record(dataset)
            writercount = writercount + 1
        message = message +  '%dMb recorded scalar data with average time of %d seconds.' % (dataset/1048576, scalartime/writercount) + '\n'
        message = message +  '%dMb recorded positional data with average time of %d seconds.' % (dataset/1048576, timepositional/writercount) + '\n'
    

        timeconsultaescalar = 0
        contadorconsultaescalar = 0
        timeconsultapositional = 0
        contadorconsultapositional = 0
    
        r.set(100, 100)
        print 'Redis scalar data query in %dMb.' % (dataset/1048576)
        while contadorconsultaescalar < generalcount:
            timeconsultaescalar = timeconsultaescalar + redis_data_search("100")
            contadorconsultaescalar = contadorconsultaescalar + 1
        message = message +  'key 100 search %dMb of scalar data with average time of %.6f seconds.' % (dataset/1048576, timeconsultaescalar/contadorconsultaescalar) + '\n'
    
        r.set("0° 0´N", "0° 0´N")
        print 'Redis positional data query in %dMb.' % (dataset/1048576)
        while contadorconsultapositional < generalcount:
            timeconsultapositional = timeconsultapositional  + redis_data_search("0° 0´N")
            contadorconsultapositional = contadorconsultapositional + 1
        message = message +  ' key 0° 0´N search %dMb of positional data with average time of %.6f seconds.' % (dataset/1048576, timeconsultapositional/contadorconsultapositional) + '\n'
        message = message +  '-------------------------------------------------------------------------------------------------------------------------------------------------\n'
        
        #Escrita multimídia
        writercountmultimidia = 0
        timeescritamultimidia = 0
        print 'Redis Multimedia data recording %dMb.' % (dataset/1048576)
        while writercountmultimidia < generalcount:
            timeescritamultimidia = timeescritamultimidia + redis_multimedia_record("directory", str(dataset) + ".mp4")
            writercountmultimidia = writercountmultimidia + 1
        message = message +  '%dMb recorded multimedia data with average time of %.6f seconds.' % (dataset/1048576, timeescritamultimidia/writercountmultimidia) + '\n'    
        
        #Consulta multimídia
        contadorconsultamultimidia = 0
        timeconsultamultimidia = 0
        r.rename("0", "teste")
        print 'Redis multimedia data query in %dMb.' % (dataset/1048576)
        while contadorconsultamultimidia < generalcount:
            timeconsultamultimidia = timeconsultamultimidia + redis_data_search("teste")
            contadorconsultamultimidia = contadorconsultamultimidia + 1
        message = message +  '%dMb searched multimedia data with average time of %.6f seconds.' % (dataset/1048576, timeconsultamultimidia/contadorconsultamultimidia) + '\n'
        message = message +  '-------------------------------------------------------------------------------------------------------------------------------------------------\n'
        print '----------------------------------------------------------------------'
        
        r.flushall()
    
    general_end = timeit.default_timer()
    t_geral = t_geral + (general_begin - general_end)/1000000
    time_geral = abs(t_geral) * 1000000
    
    message = message + 'total experiment team: %d seconds.\n' % time_geral
    
    #print message
    
    #Escreve dados no file
    file_name = "execucao_geral_redis.txt"
    if (os.path.exists(file_name)):
        os.remove(file_name)
    
    file = open(file_name,'wb+')
    file.write(message)
    file.close()
    
#################################
###  SCRIPT DE EXECUÇÃO GERAL ###
#################################
# 1 - Configure and execute all datasets in memory (generate graphs and statistical variables)
# 2 - Configure and execute all datasets on disk (generate graphs and statistical variables)
#########################
run_redis_read_write()