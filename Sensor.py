# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''


from random import randint
import timeit
import os
import sys

class SensorIoT(object):
    def __init__(self, temperature):
        self.temperature = temperature
        
    def get_temperature(self):
        return self.temperature
    
    @staticmethod
    def generate_temperatures():
        i = 0
        numbers = []
        while (i < 100):
            numbers.append(str(randint(0, 100)).strip(" "))
            i+=1
        return numbers
    
    @staticmethod
    def record_temperatures_file(file_name):
        if (os.path.exists(file_name)):
            file = open(file_name,'ab+')
        else:
            file = open(file_name,'wb+')
        # Gera um Dataset de 50MB
        size = 0
        temperatures = []
        print '>>> Begining dataset record <<<'
        while (size <= 1048576):
        #while (size <= 10000):
            size = SensorIoT.get_temperatures_file_size(file_name)   
            print 'partial dataset size: %s Bytes' % size
            #temperatures = SensorIoT.generate_temperatures()
            temperatures.append(str(randint(0, 100)).strip(" ")+',')
            file.write(''.join(temperatures))
        print '>>> dataset recorded <<<'
        print 'Dataset size: %s Bytes' % size
        file.close()
    
    @staticmethod
    def file_number_graphic_record():
        cont = 0
        numbers = []
        for cont in range (0, 36000, 500):
            numbers.append(str(cont) + "\n")
            cont += 500
        
        numbers_str = ''.join(numbers)
        
        print numbers_str
    
    @staticmethod    
    def scalar_data_generation(dataset_size):
        size = 0
        temperatures = []
        while (size <= dataset_size):
            size = sys.getsizeof(temperatures) 
            temperatures.append(randint(0, 99))
        return temperatures
    
    @staticmethod    
    def positional_data_generation(dataset_size):
        size = 0
        latitudes = []
        while (size <= dataset_size):
            size = sys.getsizeof(latitudes) 
            latitudes.append( str(randint(1, 100)) + '° ' + str(randint(1, 100))+'´N')
        return latitudes
            
    @staticmethod
    def get_temperatures_file_size(file_name):
        size = os.path.getsize(file_name)
        return size
    
    @staticmethod
    def get_temperatures(file_name):
        file = open(file_name,'rb')
        linha = file.readline()
        t = 0
        print '>>> Reading temperatures<<<'
        while linha:
            begin = timeit.default_timer()
            values = linha.strip(" ").split(',')
            end = timeit.default_timer()
            t = t + (begin - end)/1000000
            print ('partial length of recording in the collection: %f seconds' % t)
        file.close()
        print '>>> Temperature collection finalized <<<'
        print ('Duration of recording in the collection: %f minutes' % t/60)
        return values
    
