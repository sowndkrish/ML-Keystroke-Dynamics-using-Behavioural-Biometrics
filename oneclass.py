# -*- coding: utf-8 -*-
"""
Created on Tue Aug 22 14:16:05 2017

@author: user
"""

#%matplotlib inline

import numpy as np  
import pandas as pd  
from sklearn import utils  
#import matplotlib

# import the CSV from http://kdd.ics.uci.edu/databases/kddcup99/kddcup99.html
# this will return a pandas dataframe.
data = pd.read_csv('C:/Users/user/.spyder-py3/tilaug15.csv', low_memory=False)


relevant_features = [  
    "UUID",
    "Touch_Pressure",
    "Touch_Size",
    "X_Coordinate",
    "Y_Coordinate",
    "X_Precision",
    "Y_Precision",
    "Action_Timestamp",
]
# replace the data with a subset containing only the relevant features
data = data[relevant_features]

# normalise the data - this leads to better accuracy and reduces numerical instability in
# the SVM implementation
#data = np.array(data)
#label = data[:,0]
#data = data[:, [1,2,3,4,5,6,7]] / data[:, [1,2,3,4,5,6,7]].max(axis=0)
#data = np.column_stack((label,data))
#data= pd.DataFrame(data)
#data.columns =relevant_features


#df1=pd.DataFrame(relevant_features).transpose()
#print (df1)

#frames=[df1,data]
#data=pd.concat(frames)
#print(data)

#from sklearn.preprocessing import normalize
#data = normalize(data[:, [1,7]], axis=0)
# class 1 (normal) and class -1 (illegi)
data.loc[data['UUID'] == "BTGAB1500646195478", "attack"] = 1 
data.loc[data['UUID'] != "BTGAB1500646195478", "attack"] = -1
#print(data.info())

                 #print("data loc value",data['label'] != "normal.")

# grab out the attack value as the target for training and testing. since we're
# only selecting a single column from the `data` dataframe, we'll just get a
# series, not a new dataframe
target = data["attack"]
# find the proportion of outliers we expect (aka where `illegi == -1`). because 
# target is a series, we just compare against itself rather than a column.
outliers = target[target == -1]  

print("outliers.shape", outliers.shape)  
print("outlier fraction", outliers.shape[0]/target.shape[0])

data.drop(["UUID", "attack"], axis=1, inplace=True)
data.shape
indices = np.arange(data.shape[0])


from sklearn.preprocessing import StandardScaler
scaler = StandardScaler()
data = scaler.fit_transform(data)
#print(data)
from sklearn.model_selection import train_test_split  

train_data, test_data, train_target, test_target = train_test_split(data, target, train_size = 0.8,random_state=0)  
train_data.shape
from sklearn import svm

# set nu (which should be the proportion of outliers in our dataset)
nu = outliers.shape[0] / target.shape[0]  
print("nu", nu)


model = svm.OneClassSVM(cache_size=20000,nu=nu, kernel='rbf', gamma=0.00005)  
model.fit(train_data)  
print(model.fit(train_data))

from sklearn import metrics  
preds = model.predict(train_data)  
targs = train_target


print("accuracy: ", metrics.accuracy_score(targs, preds))  
print("precision: ", metrics.precision_score(targs, preds))  
print("recall: ", metrics.recall_score(targs, preds))  
print("f1: ", metrics.f1_score(targs, preds))  
print("area under curve (auc): ", metrics.roc_auc_score(targs, preds))

preds = model.predict(test_data)  
targs = test_target

print("accuracy: ", metrics.accuracy_score(targs, preds))  
print("precision: ", metrics.precision_score(targs, preds))  
print("recall: ", metrics.recall_score(targs, preds))  
print("f1: ", metrics.f1_score(targs, preds))  
print("area under curve (auc): ", metrics.roc_auc_score(targs, preds))