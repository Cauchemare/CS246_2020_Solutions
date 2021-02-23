# -*- coding: utf-8 -*-
"""
Created on Wed Jan  6 11:39:06 2021

@author: luyao.li
"""
import numpy as np 
import pandas as pd

R = np.loadtxt('F:/CS246/6W.Dimensionality Reduction-Homework/Assigment/hw2-bundle/q4/data/user-shows.txt')
COL_NAMES = pd.read_csv( 'F:/CS246/6W.Dimensionality Reduction-Homework/Assigment/hw2-bundle/q4/data/shows.txt'
                        ,encoding='utf8'
                        ,squeeze=True
                        ,header= None
                        ,names=['title']).values
    

p_diag =R.sum(axis=1)
q_diag =R.sum(axis=0)
P= np.diag(p_diag)
Q= np.diag(q_diag )

Q_1DIV2= np.diag( 1/ np.sqrt(q_diag))
P_1DIV2= np.diag( 1/ np.sqrt(p_diag))

S_I= Q_1DIV2 @ R.T @R@ Q_1DIV2
S_U = P_1DIV2 @ R @  R.T  @ P_1DIV2
Gamma_user = S_U @  R
Gamma_item = R @ S_I
user_based_ids =np.argsort( -Gamma_user[499,:100],axis=None) [:5]
item_based_ids= np.argsort( -Gamma_item[499,:100],axis=None) [:5]
print(np.take( COL_NAMES,user_based_ids ))
print(np.take( COL_NAMES,item_based_ids ))