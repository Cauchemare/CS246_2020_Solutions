# -*- coding: utf-8 -*-
"""
Created on Mon Jan  4 10:04:45 2021

@author: luyao.li
"""
import numpy as np
from math import sqrt

def main(filePath,Q_dict,P_dict):
    '''
   iterate all ,update all elements and calculate error
    '''
    error_list= []
    for i in range(num_iteration) :
        iterateElement(filePath,Q_dict,P_dict )
        error=  measureError( filePath,Q_dict,P_dict )  
        error_list.append(error) 
    return error_list
        

def iterateElement(filePath:str,Q_dict,P_dict):
    '''
    one iteration,update each element has been rated
    '''
    with open(filePath) as f:
        for line in f:
            user_id,movie_id,rate = line.split('\t')
            rate= eval(rate)
            q_i=Q_dict.get(movie_id,np.random.uniform(0,sqrt(5/k),size =(k,)))
            p_u= P_dict.get(user_id,np.random.uniform(0,sqrt(5/k),size =(k,)))
            error_iu= 2*(rate- np.dot(q_i,p_u))
            updated_qi= q_i +learning_rate*(error_iu*p_u- 2*lamb*q_i )
            updated_pu= p_u +learning_rate*(error_iu*q_i - 2*lamb*p_u )            
            Q_dict[movie_id] = updated_qi
            P_dict[user_id] =  updated_pu
    return None
    

def measureError(filePath:str,Q_dict,P_dict):
    '''
    after one iteration,based on new updated values mesure  error
    '''
    e=0
    with open(filePath) as f:
        for line in f:
            user_id,movie_id,rate=  line.split('\t')
            rate= eval(rate)
            q_i=Q_dict[movie_id]
            p_u= P_dict[user_id]
            e += (  pow( rate-  np.dot(q_i,p_u) ,2) + lamb*( pow(np.linalg.norm(q_i),2)+ pow(np.linalg.norm(p_u),2)))
    return  e
    

if  __name__ == '__main__':
    k=20
    lamb= 0.1
    learning_rate= 0.03
    num_iteration=40
    Q_dict={}
    P_dict= {}
    filePath= 'F:/CS246/6W.Dimensionality Reduction/Assigment/hw2-bundle/q3/data/ratings.train.txt'
    errs  =  main(filePath,Q_dict,P_dict)
    x=np.arange(1,num_iteration+1)
    import matplotlib.pyplot as plt
    plt.plot(x, errs, "-o")
    plt.xlabel("# of Iteration")
    plt.ylabel("Error")
    plt.title("Error vs Iteration")
    plt.show()
    '''
    
    '''
        
    
    
    
    
    
    