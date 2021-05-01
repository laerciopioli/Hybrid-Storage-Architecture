# -*- coding: UTF-8 -*-
'''
Created on set-13-2017
Last Update: april-26-2021

@author: Braulio.junior and Laercio Pioli
'''

def percentualDifCalc():
    
    group = [ [0.0015, 0.0083, 0.005],  
                    [0.0009, 0.0005, 0.006], 
                    [0.0013, 0.0006, 0.011], 
                    [0.0008, 0.0008, 0.017], 
                    [0.0011, 0.0008, 0.030], 
                    [0.0041, 0.0006, 0.105],
                    [0.0095, 0.0103, 0.429] ]
    
    #Formula: ((v2/v1) − 1) ∗ 100
    #Percentual Difference Calc between redis [0] and mongodb [1]
    x = 0
    for c in range(0, 7):
        x = x + ((group[c][1] / group[c][0]) - 1) * 100
    
    #Percentual Difference Calc between redis [0] and cassandra [2]
    y = 0
    for c in range(0, 7):
        y = y + ((group[c][2] / group[c][0]) - 1) * 100
    
    print "Redis/MongoDB had an average of %d%% superior performance than Redis/MongoDB and %d%% superior than Cassandra." %(x/7, y/7)
    
percentualDifCalc()
    