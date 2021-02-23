# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 11:12:11 2021

@author: luyao.li
"""
from abc import ABCMeta,abstractmethod 
import numpy as np


def constP(fk_1,fk):
    return  abs( fk_1- fk)*100  / fk_1

class  BaseGD(metaclass= ABCMeta):
    def __init__(self,eta=0.0000003,epis=0.25,C=100):
        self.eta = eta
        self.epis=  epis
        self.C= C
         
    @abstractmethod
    def convergenceCriteria(self,fk_1,fk):
        pass
    
    
    @abstractmethod
    def cutData(self,x,y):
        pass
    
    def updateWeight(self,xi,yi):
        #output (n_feats,1)
        '''
        yi:(N_samples,1)
        xi:(n,n_feats)
        '''
        booleanIndicator = ( yi *( self.w @  xi.T +self.b ) >=1)
        deriveW = np.where( booleanIndicator[-1,np.newaxis],0, - yi[-1,np.newaxis]* xi).sum(axis=0)
        deriveW= self.w + self.C* deriveW
        
        deriveB =np.where( booleanIndicator,0, -yi).sum(axis=0)
        self.w= self.w - self.eta * deriveW
        self.b= self.b - self.eta *deriveB
    
        
    def updateWeights(self,x,y):
        xi, yi= self.cutData(x,y)
        self.updateWeight(xi,yi)
        
    
    @abstractmethod
    def  initLogs(self):
        pass
    
    @abstractmethod
    def updateLogs(self):
        pass
    
    def fw_b(self,x,y):
        margin = 1- y *(  self.w @ x.T  +self.b)
        
        r= 0.5* ( (self.w **2).sum()) +self.C *  np.where( margin >=0,margin,0).sum()
        return r
        
    def __call__(self,x,y):
        self.n,n_feats=x.shape
        self.w = np.zeros((n_feats, ))
        self.b= 0. 
        
        self.initLogs()
        fk = self.fw_b(x,y)
        fk_1 = fk + 10000
        r= [fk]
        ks= [ self.k]
        while self.convergenceCriteria(fk_1,fk) >= self.epis:
            self.updateWeights(x,y)  #inculude w and b
            self.updateLogs()
            fk_1 = fk
            fk= self.fw_b(x,y)
            r.append(fk)
            ks.append(self.k)
            
        return r,ks 
            
class BatchGD(BaseGD):
    def convergenceCriteria(self,fk_1,fk):
        cost=   constP(fk_1 ,fk)
        return  cost
    def cutData(self,x,y):
        return x,y
    def initLogs(self):
        self.k=0
    def  updateLogs(self):
        self.k +=1
        
class  StochasticGD(BaseGD):
    def initLogs(self):
        self.i=1
        self.k= 0
        self.cost = 0
    def convergenceCriteria(self,fk_1,fk):
        self.cost=  0.5* self.cost + 0.5* constP(fk_1,fk)
        return  self.cost
    def   cutData(self,x,y):
        return x[self.i,:].reshape(1,-1) ,np.array([y[self.i]] )
    def updateLogs(self):
        self.i = (self.i % self.n) +1
        self.k +=1

class MiniBatchGD(BaseGD):  #eta= 0.0001,epis=  0.001
    def  __init__(self,eta=0.00001,epis=0.01,C=100,batchSize=20):
        super().__init__(eta,epis,C)
        self.batchSize= batchSize
    def initLogs(self):
        self.l =0
        self.k = 0
        self.cost= 0.
    def convergenceCriteria(self,fk_1,fk):
        self.cost=  0.5* self.cost + 0.5* constP(fk_1,fk)
        return self.cost
    def cutData(self ,x,y):
        start_idx= self.l * self.batchSize+1
        end_idx= min( self.n, (self.l+1)* self.batchSize +1)
        return x[ start_idx: end_idx],y[start_idx:end_idx]
    def  updateLogs(self):
        self.l = (self.l+1) % ( int(  (self.n+ self.batchSize-1)/ self.batchSize))
        self.k +=1




if  __name__ == '__main__':
    import os
    os.chdir( os.path.dirname( os.path.abspath(__file__)) )
    x =  np.loadtxt('data/features.txt',delimiter=',')
    y = np.loadtxt('data/target.txt')
    batchGD= BatchGD(eta= 0.0000003,epis= 0.25)
    stochasticGD = StochasticGD(eta= 0.0001,epis= 0.001)
    minibatchGD =MiniBatchGD(eta = 0.00001,epis =0.01,batchSize =20)
    
    from MyOutils  import Timer
    timer=Timer() 
    timer.start('batch')
    r_batch,k_batch = batchGD(x,y) 
    timer.stop('batch')
    timer.start('stochastic')
    r_stochastic,k_stochastic =stochasticGD(x,y)
    timer.stop('stochastic')
    
    timer.start('mini')
    r_minibatch ,k_minibatch =  minibatchGD(x,y)
    timer.stop('mini')

    import matplotlib.pyplot as plt
    fig = plt.figure(figsize =(15,10))
    ax =fig.add_subplot(1,1,1)
    ax.plot(k_batch,r_batch,label='batch')
    ax.plot(k_stochastic,r_stochastic,label='stochastic')
    ax.plot(k_minibatch,r_minibatch,label='mini')
    ax.set_xlabel('k',fontsize='xx-large')
    ax.set_ylabel('fk(w,b)',fontsize='xx-large')
    plt.legend(loc='upper right',fontsize='xx-large')
    plt.show()
        
    
    


        