# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 09:22:42 2021

@author: luyao.li
"""

import numpy as np 
import os
from functools import partial 
from collections import defaultdict
import matplotlib.pyplot as plt
def hash_fun(a,b,n_buckets,x, p=123457):
    y=x%p
    hash_val = (a*y+b ) %p
    return hash_val % n_buckets 
            


if __name__  == '__main__':
    
    os.chdir(os.path.dirname(os.path.abspath(__file__ )) )
    
    counts_dir= 'counts_tiny.txt'
    words_dir ='words_stream.txt'
    hash_params = np.loadtxt('hash_params.txt',delimiter='\t')
    delta = np.exp(-5)
    eps=  np.exp(1)*pow(10,-4)
    n_hash= int(  np.ceil( np.log(1/delta) ))
    n_buckets=  int(np.ceil( np.exp(1)  /eps )    )
    
    hash_list = [ partial(hash_fun,a=int(hash_params[i,0]),b=int(hash_params[i,1]),n_buckets =n_buckets )  for i  in range( n_hash )  ]

    
#    for x in data:
    
    counts_dict =[ defaultdict(int)   for i in range(len(hash_list))]
    t=0
    with open(words_dir)  as f:
        for x in f:
            x= int(x.strip())
            t+=1
            if not t%1000000:
                print(t ,'element')
            for  idx,h in enumerate(hash_list):
                h_value =  h(x=x)
                counts_dict[idx][h_value] +=1
            

    Er =[]
    wordFreq= []
    with open(counts_dir) as f:
        for l in f:
            items= l.strip().split('\t')
            word= int( items[0])
            count =int(items[1])
            Fhat= np.min( [ counts_dict[idx][ h(x=word)]  for idx,h in enumerate(hash_list)] )
            Er.append( (Fhat-count)/ count   )
            wordFreq.append(  count/t)
        

    plt.figure(figsize=(20,10))
    plt.loglog(wordFreq,Er,"+") 
    plt.title('Relative Error(log) vs Word Frequency(log)')
    plt.xlabel('Word Frequency(log)')
    plt.ylabel('Relative Error(log)')
    plt.grid()
    plt.show()
    
    
    
    
    
    
    
    
    
    
    
    
    
    