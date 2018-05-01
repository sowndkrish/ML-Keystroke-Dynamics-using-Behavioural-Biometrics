# -*- coding: utf-8 -*-
"""
Created on Sun Oct  1 22:47:33 2017

@author: user
"""


# This script takes a CSV data file of raw data and performs feature extraction.
# The script generates two files: filtered raw data and extracted features.

import csv
import sys
import numpy as np
import pandas as pd 
import re
from datetime import datetime
import math

if __name__ == '__main__':

    if len(sys.argv) != 2:
        print('Usage: python pre-processing.py profiles_2017_02_21.csv')
        sys.exit(1)

    print('# Loading data...')
    with open(sys.argv[1], 'r') as f:
        df = pd.read_csv(f, sep=',', low_memory=False, names=["ID","UUID","Language","Hardware_Model","SDK_Version","Manufacture","Screen_Size","Time_Zone","Date_Time","Country_Code","Num_of_CPU_Cores","Location","Location_lat","Location_long","Button","Touch_Pressure","Touch_Size","X_Coordinate","Y_Coordinate","X_Precision","Y_Precision","Action_Type","Action_Timestamp","HR_Timestamp"], skiprows=1)

    print('# Sorting records by UUID and Action_Timestamp...')    
    df = df.sort_values(['UUID', 'Action_Timestamp'], ascending=[True, True])
    
    print('# Removing redundant records - keeping the last value...')    
    df = df.drop_duplicates('Action_Timestamp', keep='last')    
    
    print('# Exclude records below a certain frequency count < 480 (32 counts = 1 attemp -> threashold = 15 attempt)...')    
    df = df.groupby('UUID').filter(lambda x: len(x) >= 64)
    
    print('# Delete Location data...')   
    df.drop('Location', axis=1, inplace=True)
    df.drop('Location_lat', axis=1, inplace=True)
    df.drop('Location_long', axis=1, inplace=True)

    df1 = pd.DataFrame()
    df2 = pd.DataFrame()

    ulist = pd.DataFrame(columns=('UUID', 'Action_Timestamp'))

    # 156 features
    features = pd.DataFrame(columns=("Language","Hardware_Model","SDK_Version","Manufacture","Screen_Size","Time_Zone","Country_Code","Num_of_CPU_Cores",\
                                     "pLN1","p.2","pLN3","pt4","pi5","pe6","pLN7","p58","pLN9","pSH10","pr11","po12","pa13","pn14","pl15","pDO16",\
                                     "aLN1","a.2","aLN3","at4","ai5","ae6","aLN7","a58","aLN9","aSH10","ar11","ao12","aa13","an14","al15","aDO16",\
                                     "xycLN1","xyc.2","xycyLN3","xyct4","xyci5","xyce6","xycLN7","xyc58","xycLN9","xycSH10","xycr11","xyco12","xyca13","xycn14","xycl15","xycDO16",\
                                     "xypLN1","xyp.2","xypLN3","xypt4","xypi5","xype6","xypLN7","xyp58","xypLN9","xypSH10","xypr11","xypo12","xypa13","xypn14","xypl15","xypDO16",\
                                     "duLN1","du.2","duLN3","dut4","dui5","due6","duLN7","du58","duLN9","duSH10","dur11","duo12","dua13","dun14","dul15","duDO16",\
                                     "udLN1","ud.2","udLN3","udt4","udi5","ude6","udLN7","ud58","udLN9","udSH10","udr11","udo12","uda13","udn14","udl15",\
                                     "ddLN1","dd.2","ddLN3","ddt4","ddi5","dde6","ddLN7","dd58","ddLN9","ddSH10","ddr11","ddo12","dda13","ddn14","ddl15",\
                                     "uuLN1","uu.2","uuLN3","uut4","uui5","uue6","uuLN7","uu58","uuLN9","uuSH10","uur11","uuo12","uua13","uun14","uul15",\
                                     "du2LN1","du2.2","du2LN3","du2t4","du2i5","du2e6","du2LN7","du258","du2LN9","du2SH10","du2r11","du2o12","du2a13","du2n14","du2n15",\
                                     "avgdu","avgud","avgdd","avguu","avdu2","avgp","avga","UUID"\
                                     ))

    record_button = ['LETTERS','LETTERS','.','.','LETTERS','NUMBERS','t','t','i','i','e','e','LETTERS','LETTERS','5','5','LETTERS','NUMBERS','SHIFT','SHIFT','R','r','o','o','a','a','n','n','l','l','DONE','DONE']
    record_event = ['Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up','Down','Up']
    print('# Parsing ...')

    # with pd.option_context('display.max_rows', 80, 'display.max_columns', 25):
    #    pd.set_option('max_colwidth', 300);
    #    print (df)
        
    print('# Found ' + str(len(df))  + ' records for touch events')

    counter = 0
    for i in range(0, len(df)):
        if i+59 < len(df):
            if (df.iloc[i]["Button"] == 'LETTERS' or df.iloc[i]["Button"] == 'NUMBERS') and df.iloc[i]["Action_Type"] == 'Down' and \
               (df.iloc[i+1]["Button"] == 'LETTERS' or df.iloc[i+1]["Button"] == 'NUMBERS') and df.iloc[i+1]["Action_Type"] == 'Up' or \
               df.iloc[i+2]["Button"] == 'BACKSPACE' and df.iloc[i+2]["Action_Type"] == 'Down' and \
               df.iloc[i+3]["Button"] == 'BACKSPACE' and df.iloc[i+3]["Action_Type"] == 'Up' and \
               df.iloc[i+4]["Button"] == '.' and df.iloc[i+4]["Action_Type"] == 'Down' and \
               df.iloc[i+5]["Button"] == '.' and df.iloc[i+5]["Action_Type"] == 'Up' or \
               df.iloc[i+6]["Button"] == 'BACKSPACE' and df.iloc[i+6]["Action_Type"] == 'Down' and \
               df.iloc[i+7]["Button"] == 'BACKSPACE' and df.iloc[i+7]["Action_Type"] == 'Up' and \
               (df.iloc[i+8]["Button"] == 'LETTERS' or df.iloc[i+8]["Button"] == 'NUMBERS') and df.iloc[i+8]["Action_Type"] == 'Down' and \
               (df.iloc[i+9]["Button"] == 'LETTERS' or df.iloc[i+9]["Button"] == 'NUMBERS') and df.iloc[i+9]["Action_Type"] == 'Up' or \
               df.iloc[i+10]["Button"] == 'BACKSPACE' and df.iloc[i+10]["Action_Type"] == 'Down' and \
               df.iloc[i+11]["Button"] == 'BACKSPACE' and df.iloc[i+11]["Action_Type"] == 'Up' and \
               df.iloc[i+12]["Button"] == 't' and df.iloc[i+12]["Action_Type"] == 'Down' and \
               df.iloc[i+13]["Button"] == 't' and df.iloc[i+13]["Action_Type"] == 'Up'or \
               df.iloc[i+14]["Button"] == 'BACKSPACE' and df.iloc[i+14]["Action_Type"] == 'Down' and \
               df.iloc[i+15]["Button"] == 'BACKSPACE' and df.iloc[i+15]["Action_Type"] == 'Up' and \
               df.iloc[i+16]["Button"] == 'i' and df.iloc[i+16]["Action_Type"] == 'Down' and \
               df.iloc[i+17]["Button"] == 'i' and df.iloc[i+17]["Action_Type"] == 'Up'or \
               df.iloc[i+18]["Button"] == 'BACKSPACE' and df.iloc[i+18]["Action_Type"] == 'Down' and \
               df.iloc[i+19]["Button"] == 'BACKSPACE' and df.iloc[i+19]["Action_Type"] == 'Up' and \
               df.iloc[i+20]["Button"] == 'e' and df.iloc[i+20]["Action_Type"] == 'Down' and \
               df.iloc[i+21]["Button"] == 'e' and df.iloc[i+21]["Action_Type"] == 'Up' or \
               df.iloc[i+22]["Button"] == 'BACKSPACE' and df.iloc[i+22]["Action_Type"] == 'Down' and \
               df.iloc[i+23]["Button"] == 'BACKSPACE' and df.iloc[i+23]["Action_Type"] == 'Up' and \
               (df.iloc[i+24]["Button"] == 'LETTERS' or df.iloc[i+24]["Button"] == 'NUMBERS') and df.iloc[i+24]["Action_Type"] == 'Down' and \
               (df.iloc[i+25]["Button"] == 'LETTERS' or df.iloc[i+25]["Button"] == 'NUMBERS') and df.iloc[i+25]["Action_Type"] == 'Up' or \
               df.iloc[i+26]["Button"] == 'BACKSPACE' and df.iloc[i+26]["Action_Type"] == 'Down' and \
               df.iloc[i+27]["Button"] == 'BACKSPACE' and df.iloc[i+27]["Action_Type"] == 'Up' and \
               df.iloc[i+28]["Button"] == '5' and df.iloc[i+28]["Action_Type"] == 'Down' and \
               df.iloc[i+29]["Button"] == '5' and df.iloc[i+29]["Action_Type"] == 'Up' or \
               df.iloc[i+30]["Button"] == 'BACKSPACE' and df.iloc[i+30]["Action_Type"] == 'Down' and \
               df.iloc[i+31]["Button"] == 'BACKSPACE' and df.iloc[i+31]["Action_Type"] == 'Up' and \
               (df.iloc[i+32]["Button"] == 'LETTERS' or df.iloc[i+32]["Button"] == 'NUMBERS') and df.iloc[i+32]["Action_Type"] == 'Down' and \
               (df.iloc[i+33]["Button"] == 'LETTERS' or df.iloc[i+33]["Button"] == 'NUMBERS') and df.iloc[i+33]["Action_Type"] == 'Up' or \
               df.iloc[i+34]["Button"] == 'BACKSPACE' and df.iloc[i+34]["Action_Type"] == 'Down' and \
               df.iloc[i+35]["Button"] == 'BACKSPACE' and df.iloc[i+35]["Action_Type"] == 'Up' and \
               df.iloc[i+36]["Button"] == 'SHIFT' and df.iloc[i+36]["Action_Type"] == 'Down' and \
               df.iloc[i+37]["Button"] == 'SHIFT' and df.iloc[i+37]["Action_Type"] == 'Up' or \
               df.iloc[i+38]["Button"] == 'BACKSPACE' and df.iloc[i+38]["Action_Type"] == 'Down' and \
               df.iloc[i+39]["Button"] == 'BACKSPACE' and df.iloc[i+39]["Action_Type"] == 'Up' and \
               (df.iloc[i+40]["Button"] == 'R' or df.iloc[i+40]["Button"] == 'r' ) and df.iloc[i+40]["Action_Type"] == 'Down' and \
               (df.iloc[i+41]["Button"] == 'r' or df.iloc[i+41]["Button"] == 'R') and df.iloc[i+41]["Action_Type"] == 'Up' or \
               df.iloc[i+42]["Button"] == 'BACKSPACE' and df.iloc[i+42]["Action_Type"] == 'Down' and \
               df.iloc[i+43]["Button"] == 'BACKSPACE' and df.iloc[i+43]["Action_Type"] == 'Up' and \
               df.iloc[i+44]["Button"] == 'o' and df.iloc[i+44]["Action_Type"] == 'Down' and \
               df.iloc[i+45]["Button"] == 'o' and df.iloc[i+45]["Action_Type"] == 'Up' or \
               df.iloc[i+46]["Button"] == 'BACKSPACE' and df.iloc[i+46]["Action_Type"] == 'Down' and \
               df.iloc[i+47]["Button"] == 'BACKSPACE' and df.iloc[i+47]["Action_Type"] == 'Up' and \
               df.iloc[i+48]["Button"] == 'a' and df.iloc[i+48]["Action_Type"] == 'Down' and \
               df.iloc[i+49]["Button"] == 'a' and df.iloc[i+49]["Action_Type"] == 'Up' or \
               df.iloc[i+50]["Button"] == 'BACKSPACE' and df.iloc[i+50]["Action_Type"] == 'Down' and \
               df.iloc[i+51]["Button"] == 'BACKSPACE' and df.iloc[i+51]["Action_Type"] == 'Up' and \
               df.iloc[i+52]["Button"] == 'n' and df.iloc[i+52]["Action_Type"] == 'Down' and \
               df.iloc[i+53]["Button"] == 'n' and df.iloc[i+53]["Action_Type"] == 'Up' or \
               df.iloc[i+54]["Button"] == 'BACKSPACE' and df.iloc[i+54]["Action_Type"] == 'Down' and \
               df.iloc[i+55]["Button"] == 'BACKSPACE' and df.iloc[i+55]["Action_Type"] == 'Up' and \
               df.iloc[i+56]["Button"] == 'l' and df.iloc[i+56]["Action_Type"] == 'Down' and \
               df.iloc[i+57]["Button"] == 'l' and df.iloc[i+57]["Action_Type"] == 'Up' and \
               df.iloc[i+58]["Button"] == 'DONE' and df.iloc[i+58]["Action_Type"] == 'Down' and \
               df.iloc[i+59]["Button"] == 'DONE' and df.iloc[i+59]["Action_Type"] == 'Up' :
                   
                ulist.loc[len(ulist)]=[df.iloc[i]["UUID"],df.iloc[i]["Action_Timestamp"]]
                counter = counter +1
                df1 = df1.append(df.iloc[i:i+60], ignore_index=True)

    print('# Found ' + str(counter) +' correct instances')
    del_rows = np.where(df["Button"] == "DELETE")
    print('# Found ' + str(np.shape(del_rows)[1]) + ' DELETE events')
    users_count = ulist.groupby(['UUID']).Action_Timestamp.nunique()
    users_count = users_count.sort_values()
    aList = []
    for x in range(0,len(users_count)):
        if (users_count[x]>=30): # find users with minimum 10 correct instances
            aList.append(users_count.keys()[x])

    print('# Building a dataframe for ' + str(len(aList)) + ' users')

    for id in aList:
        print('# Adding instances for User ' + str(id))
        idxn = 0
        for i in range(0, len(df1)):
            if df1.iloc[i]["UUID"] == id and (df1.iloc[i]["Button"] == 'LETTERS' or df1.iloc[i]["Button"] == 'NUMBERS') and df1.iloc[i]["Action_Type"] == 'Down' and idxn < 30:
                df2 = df2.append(df1.iloc[i:i+60], ignore_index=True)
                features.loc[len(features)] = [\
                                            df1.iloc[i]["Language"],\
                                            df1.iloc[i]["Hardware_Model"],\
                                            df1.iloc[i]["SDK_Version"],\
                                            df1.iloc[i]["Manufacture"],\
                                            df1.iloc[i]["Screen_Size"],\
                                            df1.iloc[i]["Time_Zone"],\
                                            df1.iloc[i]["Country_Code"],\
                                            df1.iloc[i]["Num_of_CPU_Cores"],\
                                            df1.iloc[i]["Touch_Pressure"],\
                                            df1.iloc[i+4]["Touch_Pressure"],\
                                            df1.iloc[i+8]["Touch_Pressure"],\
                                            df1.iloc[i+12]["Touch_Pressure"],\
                                            df1.iloc[i+16]["Touch_Pressure"],\
                                            df1.iloc[i+20]["Touch_Pressure"],\
                                            df1.iloc[i+24]["Touch_Pressure"],\
                                            df1.iloc[i+28]["Touch_Pressure"],\
                                            df1.iloc[i+32]["Touch_Pressure"],\
                                            df1.iloc[i+36]["Touch_Pressure"],\
                                            df1.iloc[i+40]["Touch_Pressure"],\
                                            df1.iloc[i+44]["Touch_Pressure"],\
                                            df1.iloc[i+48]["Touch_Pressure"],\
                                            df1.iloc[i+52]["Touch_Pressure"],\
                                            df1.iloc[i+56]["Touch_Pressure"],\
                                            df1.iloc[i+58]["Touch_Pressure"],\
                                            df1.iloc[i]["Touch_Size"],\
                                            df1.iloc[i+4]["Touch_Size"],\
                                            df1.iloc[i+8]["Touch_Size"],\
                                            df1.iloc[i+12]["Touch_Size"],\
                                            df1.iloc[i+16]["Touch_Size"],\
                                            df1.iloc[i+20]["Touch_Size"],\
                                            df1.iloc[i+24]["Touch_Size"],\
                                            df1.iloc[i+28]["Touch_Size"],\
                                            df1.iloc[i+32]["Touch_Size"],\
                                            df1.iloc[i+36]["Touch_Size"],\
                                            df1.iloc[i+40]["Touch_Size"],\
                                            df1.iloc[i+44]["Touch_Size"],\
                                            df1.iloc[i+48]["Touch_Size"],\
                                            df1.iloc[i+52]["Touch_Size"],\
                                            df1.iloc[i+56]["Touch_Size"],\
                                            df1.iloc[i+58]["Touch_Size"],\
                                            math.sqrt(math.pow(df1.iloc[i]["X_Coordinate"], 2)+math.pow(df1.iloc[i]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+4]["X_Coordinate"], 2)+math.pow(df1.iloc[i+4]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+8]["X_Coordinate"], 2)+math.pow(df1.iloc[i+8]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+12]["X_Coordinate"], 2)+math.pow(df1.iloc[i+12]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+16]["X_Coordinate"], 2)+math.pow(df1.iloc[i+16]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+20]["X_Coordinate"], 2)+math.pow(df1.iloc[i+20]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+24]["X_Coordinate"], 2)+math.pow(df1.iloc[i+24]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+28]["X_Coordinate"], 2)+math.pow(df1.iloc[i+28]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+32]["X_Coordinate"], 2)+math.pow(df1.iloc[i+32]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+36]["X_Coordinate"], 2)+math.pow(df1.iloc[i+36]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+40]["X_Coordinate"], 2)+math.pow(df1.iloc[i+40]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+44]["X_Coordinate"], 2)+math.pow(df1.iloc[i+44]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+48]["X_Coordinate"], 2)+math.pow(df1.iloc[i+48]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+52]["X_Coordinate"], 2)+math.pow(df1.iloc[i+52]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+56]["X_Coordinate"], 2)+math.pow(df1.iloc[i+56]["Y_Coordinate"], 2)),\
                                            math.sqrt(math.pow(df1.iloc[i+58]["X_Coordinate"], 2)+math.pow(df1.iloc[i+58]["Y_Coordinate"], 2)),\
                                            df1.iloc[i]["X_Precision"] + df1.iloc[i]["Y_Precision"],\
                                            df1.iloc[i+4]["X_Precision"] + df1.iloc[i+4]["Y_Precision"],\
                                            df1.iloc[i+8]["X_Precision"] + df1.iloc[i+8]["Y_Precision"],\
                                            df1.iloc[i+12]["X_Precision"] + df1.iloc[i+12]["Y_Precision"],\
                                            df1.iloc[i+16]["X_Precision"] + df1.iloc[i+16]["Y_Precision"],\
                                            df1.iloc[i+20]["X_Precision"] + df1.iloc[i+20]["Y_Precision"],\
                                            df1.iloc[i+24]["X_Precision"] + df1.iloc[i+24]["Y_Precision"],\
                                            df1.iloc[i+28]["X_Precision"] + df1.iloc[i+28]["Y_Precision"],\
                                            df1.iloc[i+32]["X_Precision"] + df1.iloc[i+32]["Y_Precision"],\
                                            df1.iloc[i+36]["X_Precision"] + df1.iloc[i+36]["Y_Precision"],\
                                            df1.iloc[i+40]["X_Precision"] + df1.iloc[i+40]["Y_Precision"],\
                                            df1.iloc[i+44]["X_Precision"] + df1.iloc[i+44]["Y_Precision"],\
                                            df1.iloc[i+48]["X_Precision"] + df1.iloc[i+48]["Y_Precision"],\
                                            df1.iloc[i+52]["X_Precision"] + df1.iloc[i+52]["Y_Precision"],\
                                            df1.iloc[i+56]["X_Precision"] + df1.iloc[i+56]["Y_Precision"],\
                                            df1.iloc[i+58]["X_Precision"] + df1.iloc[i+58]["Y_Precision"],\
                                            df1.iloc[i+1]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"],\
                                            df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"],\
                                            df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"],\
                                            df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"],\
                                            df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"],\
                                            df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"],\
                                            df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"],\
                                            df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"],\
                                            df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"],\
                                            df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"],\
                                            df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"],\
                                            df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"],\
                                            df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"],\
                                            df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"],\
                                            df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"],\
                                            df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+58]["Action_Timestamp"],\
                                            df1.iloc[i+4]["Action_Timestamp"] - df1.iloc[i+1]["Action_Timestamp"],\
                                            df1.iloc[i+8]["Action_Timestamp"] - df1.iloc[i+5]["Action_Timestamp"],\
                                            df1.iloc[i+12]["Action_Timestamp"] - df1.iloc[i+9]["Action_Timestamp"],\
                                            df1.iloc[i+16]["Action_Timestamp"] - df1.iloc[i+13]["Action_Timestamp"],\
                                            df1.iloc[i+20]["Action_Timestamp"] - df1.iloc[i+17]["Action_Timestamp"],\
                                            df1.iloc[i+24]["Action_Timestamp"] - df1.iloc[i+21]["Action_Timestamp"],\
                                            df1.iloc[i+28]["Action_Timestamp"] - df1.iloc[i+25]["Action_Timestamp"],\
                                            df1.iloc[i+32]["Action_Timestamp"] - df1.iloc[i+29]["Action_Timestamp"],\
                                            df1.iloc[i+36]["Action_Timestamp"] - df1.iloc[i+33]["Action_Timestamp"],\
                                            df1.iloc[i+40]["Action_Timestamp"] - df1.iloc[i+37]["Action_Timestamp"],\
                                            df1.iloc[i+44]["Action_Timestamp"] - df1.iloc[i+41]["Action_Timestamp"],\
                                            df1.iloc[i+48]["Action_Timestamp"] - df1.iloc[i+45]["Action_Timestamp"],\
                                            df1.iloc[i+52]["Action_Timestamp"] - df1.iloc[i+49]["Action_Timestamp"],\
                                            df1.iloc[i+56]["Action_Timestamp"] - df1.iloc[i+53]["Action_Timestamp"],\
                                            df1.iloc[i+58]["Action_Timestamp"] - df1.iloc[i+57]["Action_Timestamp"],\
                                            df1.iloc[i+4]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"],\
                                            df1.iloc[i+8]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"],\
                                            df1.iloc[i+12]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"],\
                                            df1.iloc[i+16]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"],\
                                            df1.iloc[i+20]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"],\
                                            df1.iloc[i+24]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"],\
                                            df1.iloc[i+28]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"],\
                                            df1.iloc[i+32]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"],\
                                            df1.iloc[i+36]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"],\
                                            df1.iloc[i+40]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"],\
                                            df1.iloc[i+44]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"],\
                                            df1.iloc[i+48]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"],\
                                            df1.iloc[i+52]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"],\
                                            df1.iloc[i+56]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"],\
                                            df1.iloc[i+58]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"],\
                                            df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i+1]["Action_Timestamp"],\
                                            df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+5]["Action_Timestamp"],\
                                            df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+9]["Action_Timestamp"],\
                                            df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+13]["Action_Timestamp"],\
                                            df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+17]["Action_Timestamp"],\
                                            df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+21]["Action_Timestamp"],\
                                            df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+25]["Action_Timestamp"],\
                                            df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+29]["Action_Timestamp"],\
                                            df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+33]["Action_Timestamp"],\
                                            df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+37]["Action_Timestamp"],\
                                            df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+41]["Action_Timestamp"],\
                                            df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+45]["Action_Timestamp"],\
                                            df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+49]["Action_Timestamp"],\
                                            df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+53]["Action_Timestamp"],\
                                            df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+57]["Action_Timestamp"],\
                                            df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"],\
                                            df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"],\
                                            df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"],\
                                            df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"],\
                                            df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"],\
                                            df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"],\
                                            df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"],\
                                            df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"],\
                                            df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"],\
                                            df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"],\
                                            df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"],\
                                            df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"],\
                                            df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"],\
                                            df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"],\
                                            df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"],\
                                            ((df1.iloc[i+1]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"])+\
                                            (df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"])+\
                                            (df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"])+\
                                            (df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"])+\
                                            (df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"])+\
                                            (df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"])+\
                                            (df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"])+\
                                            (df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"])+\
                                            (df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"])+\
                                            (df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"])+\
                                            (df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"])+\
                                            (df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"])+\
                                            (df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"])+\
                                            (df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"])+\
                                            (df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"])+\
                                            (df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+58]["Action_Timestamp"]))/16,\
                                            ((df1.iloc[i+4]["Action_Timestamp"] - df1.iloc[i+1]["Action_Timestamp"])+\
                                            (df1.iloc[i+8]["Action_Timestamp"] - df1.iloc[i+5]["Action_Timestamp"])+\
                                            (df1.iloc[i+12]["Action_Timestamp"] - df1.iloc[i+9]["Action_Timestamp"])+\
                                            (df1.iloc[i+16]["Action_Timestamp"] - df1.iloc[i+13]["Action_Timestamp"])+\
                                            (df1.iloc[i+20]["Action_Timestamp"] - df1.iloc[i+17]["Action_Timestamp"])+\
                                            (df1.iloc[i+24]["Action_Timestamp"] - df1.iloc[i+21]["Action_Timestamp"])+\
                                            (df1.iloc[i+28]["Action_Timestamp"] - df1.iloc[i+25]["Action_Timestamp"])+\
                                            (df1.iloc[i+32]["Action_Timestamp"] - df1.iloc[i+29]["Action_Timestamp"])+\
                                            (df1.iloc[i+36]["Action_Timestamp"] - df1.iloc[i+33]["Action_Timestamp"])+\
                                            (df1.iloc[i+40]["Action_Timestamp"] - df1.iloc[i+37]["Action_Timestamp"])+\
                                            (df1.iloc[i+44]["Action_Timestamp"] - df1.iloc[i+41]["Action_Timestamp"])+\
                                            (df1.iloc[i+48]["Action_Timestamp"] - df1.iloc[i+45]["Action_Timestamp"])+\
                                            (df1.iloc[i+52]["Action_Timestamp"] - df1.iloc[i+49]["Action_Timestamp"])+\
                                            (df1.iloc[i+56]["Action_Timestamp"] - df1.iloc[i+53]["Action_Timestamp"])+\
                                            (df1.iloc[i+58]["Action_Timestamp"] - df1.iloc[i+57]["Action_Timestamp"]))/15,\
                                            ((df1.iloc[i+4]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"])+\
                                            (df1.iloc[i+8]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"])+\
                                            (df1.iloc[i+12]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"])+\
                                            (df1.iloc[i+16]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"])+\
                                            (df1.iloc[i+20]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"])+\
                                            (df1.iloc[i+24]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"])+\
                                            (df1.iloc[i+28]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"])+\
                                            (df1.iloc[i+32]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"])+\
                                            (df1.iloc[i+36]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"])+\
                                            (df1.iloc[i+40]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"])+\
                                            (df1.iloc[i+44]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"])+\
                                            (df1.iloc[i+48]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"])+\
                                            (df1.iloc[i+52]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"])+\
                                            (df1.iloc[i+56]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"])+\
                                            (df1.iloc[i+58]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"]))/15,\
                                            ((df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i+1]["Action_Timestamp"])+\
                                            (df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+5]["Action_Timestamp"])+\
                                            (df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+9]["Action_Timestamp"])+\
                                            (df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+13]["Action_Timestamp"])+\
                                            (df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+17]["Action_Timestamp"])+\
                                            (df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+21]["Action_Timestamp"])+\
                                            (df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+25]["Action_Timestamp"])+\
                                            (df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+29]["Action_Timestamp"])+\
                                            (df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+33]["Action_Timestamp"])+\
                                            (df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+37]["Action_Timestamp"])+\
                                            (df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+41]["Action_Timestamp"])+\
                                            (df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+45]["Action_Timestamp"])+\
                                            (df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+49]["Action_Timestamp"])+\
                                            (df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+53]["Action_Timestamp"])+\
                                            (df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+57]["Action_Timestamp"]))/15,\
                                            ((df1.iloc[i+5]["Action_Timestamp"] - df1.iloc[i]["Action_Timestamp"])+\
                                            (df1.iloc[i+9]["Action_Timestamp"] - df1.iloc[i+4]["Action_Timestamp"])+\
                                            (df1.iloc[i+13]["Action_Timestamp"] - df1.iloc[i+8]["Action_Timestamp"])+\
                                            (df1.iloc[i+17]["Action_Timestamp"] - df1.iloc[i+12]["Action_Timestamp"])+\
                                            (df1.iloc[i+21]["Action_Timestamp"] - df1.iloc[i+16]["Action_Timestamp"])+\
                                            (df1.iloc[i+25]["Action_Timestamp"] - df1.iloc[i+20]["Action_Timestamp"])+\
                                            (df1.iloc[i+29]["Action_Timestamp"] - df1.iloc[i+24]["Action_Timestamp"])+\
                                            (df1.iloc[i+33]["Action_Timestamp"] - df1.iloc[i+28]["Action_Timestamp"])+\
                                            (df1.iloc[i+37]["Action_Timestamp"] - df1.iloc[i+32]["Action_Timestamp"])+\
                                            (df1.iloc[i+41]["Action_Timestamp"] - df1.iloc[i+36]["Action_Timestamp"])+\
                                            (df1.iloc[i+45]["Action_Timestamp"] - df1.iloc[i+40]["Action_Timestamp"])+\
                                            (df1.iloc[i+49]["Action_Timestamp"] - df1.iloc[i+44]["Action_Timestamp"])+\
                                            (df1.iloc[i+53]["Action_Timestamp"] - df1.iloc[i+48]["Action_Timestamp"])+\
                                            (df1.iloc[i+57]["Action_Timestamp"] - df1.iloc[i+52]["Action_Timestamp"])+\
                                            (df1.iloc[i+59]["Action_Timestamp"] - df1.iloc[i+56]["Action_Timestamp"]))/15,\
                                            ((df1.iloc[i]["Touch_Pressure"])+\
                                            (df1.iloc[i+4]["Touch_Pressure"])+\
                                            (df1.iloc[i+8]["Touch_Pressure"])+\
                                            (df1.iloc[i+12]["Touch_Pressure"])+\
                                            (df1.iloc[i+16]["Touch_Pressure"])+\
                                            (df1.iloc[i+20]["Touch_Pressure"])+\
                                            (df1.iloc[i+24]["Touch_Pressure"])+\
                                            (df1.iloc[i+28]["Touch_Pressure"])+\
                                            (df1.iloc[i+32]["Touch_Pressure"])+\
                                            (df1.iloc[i+36]["Touch_Pressure"])+\
                                            (df1.iloc[i+40]["Touch_Pressure"])+\
                                            (df1.iloc[i+44]["Touch_Pressure"])+\
                                            (df1.iloc[i+48]["Touch_Pressure"])+\
                                            (df1.iloc[i+52]["Touch_Pressure"])+\
                                            (df1.iloc[i+56]["Touch_Pressure"])+\
                                            (df1.iloc[i+58]["Touch_Pressure"]))/16,\
                                            ((df1.iloc[i]["Touch_Size"])+\
                                            (df1.iloc[i+4]["Touch_Size"])+\
                                            (df1.iloc[i+8]["Touch_Size"])+\
                                            (df1.iloc[i+12]["Touch_Size"])+\
                                            (df1.iloc[i+16]["Touch_Size"])+\
                                            (df1.iloc[i+20]["Touch_Size"])+\
                                            (df1.iloc[i+24]["Touch_Size"])+\
                                            (df1.iloc[i+28]["Touch_Size"])+\
                                            (df1.iloc[i+32]["Touch_Size"])+\
                                            (df1.iloc[i+36]["Touch_Size"])+\
                                            (df1.iloc[i+40]["Touch_Size"])+\
                                            (df1.iloc[i+44]["Touch_Size"])+\
                                            (df1.iloc[i+48]["Touch_Size"])+\
                                            (df1.iloc[i+52]["Touch_Size"])+\
                                            (df1.iloc[i+56]["Touch_Size"])+\
                                            (df1.iloc[i+58]["Touch_Size"]))/16,\
                                            id\
                                            ]
                idxn = idxn +1
                i = i + 60

                
                
    print('# Created ' + str(df2.shape) +' dataframe')
    df2.to_csv('increased30raw_data_filtered.csv', sep='\t', encoding='utf-8')
    print('# File generated: increased30raw_data_filtered.csv')
    # print(features.head(5))
    # print(features.iloc[1:3]["duLN1"])
    # features = features.astype(str)
    features.to_csv('increased300featuresfile.csv', sep=',', encoding ='utf-8', index=False)
    # for column in features.columns:
    #    print('{0} {1}'.format(str(type(features[column][0])),str(column)))
    print('# File generated: increased30features.csv')
