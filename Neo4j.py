# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''

from neo4j.v1 import GraphDatabase, basic_auth
from Sensor import SensorIoT
import timeit
import math

# Abre a conexao
uri = "bolt://localhost:7687"
driver = GraphDatabase.driver(uri, auth=basic_auth("neo4j", "1234"))
session = driver.session()

def neo4j_scalar_data_recording(tam_dataset):
        t = 0
        
        temperatures = SensorIoT.gerar_dados_escalares(tam_dataset)
        
        result = session.run('match (sensor) where sensor:temperatura return count(*)')
        cont = result.single()[0]
        cont += 1
        
        for temperature in temperatures:
            begin = timeit.default_timer()
            
            # insere um novo noh no neo4j
            session.run("CREATE (sensor:temperatura {id: {id}, temperatura: {temperatura}})",
                        {"id": cont, "temperatura": temperature})
            
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            time = abs(t) * 1000000
            cont += 1

        return '%s recorded temperatures in %d seconds. ' % (len(temperatures), time)

def neo4j_positional_data_recording():
        t = 0
                
        print '>>> bbegin of latitudes in memory generation <<<'
        latitudes = SensorIoT.gerar_dados_posicionais()
        print '>>> end of latitudes in memory generation<<<'
        
        # Obtem total de nos
        result = session.run('match (sensor) where sensor:sp return count(*)')
        cont = result.single()[0]
        cont += 1
        
        for latitude in latitudes:
            begin = timeit.default_timer()
            
            # insere um novo noh no neo4j
            session.run("CREATE (sensor:sp {id: {id}, latitude: {latitude}})",
                        {"id": cont, "latitude": latitude})
            
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            cont += 1
            print 'partial duration of recording on neo4j: %f segundos' % t
        print '>>> end of recording latitudes in neo4j <<<'
        t = t/60
        print 'duration of recording on neo4j: %f minutos' % t
        print 'total latitudes recorded on neo4j: %s' % len(latitudes)

        session.close()

def neo4j_data_search(chave):
    begin = timeit.default_timer()
    res = session.run("match (sensor) where sensor:st and sensor.temperatura = 0 return count(*)")
    end = timeit.default_timer()
    t = (begin - end)/1000000
    time = abs(t)
        
    if res.single()[0] > 0:
        return 'key %s found in %f seconds.' % (chave, time)
    else:
        return 'Register s% not found.' % str(chave)

datasets = [1048576]
mensagem = ""

for dataset in datasets:
    
    mensagem = mensagem + 'Dataset de %dMB: ' % math.floor(dataset/1048576)
    mensagem = mensagem + neo4j_scalar_data_recording(dataset)

print mensagem
