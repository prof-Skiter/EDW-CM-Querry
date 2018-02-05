# -*- coding: utf-8 -*-
"""
Created on Fri Jan 19 11:34:28 2018

@author: skiter
"""

from sqlalchemy import create_engine
import pandas as pd
import datetime

engine = create_engine('mssql+pyodbc://sskiter1:sskiter1@edw123@PRDEDWPLYW2SQL0.corporate.t-mobile.com/DM_OMC_Nokia_NetAct?driver=SQL+Server+Native+Client+11.0')
connection = engine.connect()

# GET RMOD
stmt = 'SELECT [OBJ].[CO_DN], [RMOD_R_PRODUCT_NAME] FROM [DM_OMC_Nokia_NetAct].[dbo].[stage_T4OSS_C_SRER_RMOD_R] AS RMOD LEFT JOIN [dbo].[stage_T4OSS_CTP_COMMON_OBJECTS] AS OBJ ON RMOD.OBJ_GID = OBJ.CO_GID'
results = connection.execute(stmt).fetchall()
keys = results[0].keys()

for i, rec in enumerate(results):
    results[i] = [rec[0].split(sep='/')[1].replace('MRBTS-', ''), rec[1]]

df = pd.DataFrame(results)
df.columns = keys
RMOD = df\
    .fillna('X')\
    .groupby('CO_DN')['RMOD_R_PRODUCT_NAME']\
    .apply(lambda x: "{%s}" % ' '\
    .join(['x'.join([str((x == i).sum()), i]) for i in set(x)]))
    
RMOD = pd.DataFrame(RMOD).reset_index(level=0)
RMOD.rename(columns={'CO_DN':'MRBTS'}, inplace=True)
print('RMOD done')

#GET RF SHARING
stmt = "SELECT MNL.[MNL_R_5R64499SRT], (SELECT value FROM STRING_SPLIT (OBJ.[CO_DN], '/') WHERE value LIKE 'MR%') AS MRBTS FROM [DM_OMC_Nokia_NetAct].[dbo].[stage_T4OSS_C_SRM_MNL_R] AS MNL LEFT JOIN [dbo].[stage_T4OSS_CTP_COMMON_OBJECTS] AS OBJ ON MNL.OBJ_GID = OBJ.CO_GID"
results = connection.execute(stmt).fetchall()
df = pd.DataFrame(results)
df.columns = ['Rfsharing', 'MRBTS']
df.Rfsharing = df.Rfsharing.map({0:'none', 1:'UTRAN-EUTRA', 2:'UTRAN-GERAN', 3:'EUTRA-GERAN', 4:'UTRAN-GERAN/CONCURRENT', 5:'EUTRA-GERAN/CONCURRENT', 6:'EUTRA-EUTRA', 7:'EUTRA-EUTRA/EUTRA-GERAN', 8:'EUTRA-CDMA'})
df.MRBTS = df.MRBTS.str.replace('MRBTS-', '')
RFSH = df
print('RF SHARING done')

# GET TXRX
stmt = "SELECT CH.[CH_DIRECTION],(SELECT value FROM STRING_SPLIT (OBJ.[CO_DN], '/') WHERE value LIKE 'MR%') AS MRBTS,(SELECT value FROM STRING_SPLIT (OBJ.[CO_DN], '/') WHERE value LIKE 'LC%') AS LCELL FROM [DM_OMC_Nokia_NetAct].[dbo].[stage_T4OSS_C_SRM_CH] AS CH LEFT JOIN [dbo].[stage_T4OSS_CTP_COMMON_OBJECTS] AS OBJ ON CH.OBJ_GID = OBJ.CO_GID"
results = connection.execute(stmt).fetchall()
keys = results[0].keys()
df = pd.DataFrame(results)
df.columns = keys

df.CH_DIRECTION = df.CH_DIRECTION.map({1:'TX', 2:'RX'})
df.MRBTS = df.MRBTS.str.replace('MRBTS-', '')
df.LCELL = df.LCELL.str.replace('LCELL-', '')
df['CellID'] = df.MRBTS + '-' + df.LCELL
TXRX = df.groupby('CellID')['CH_DIRECTION'].apply(lambda x: "{%s}" % ''.join([''.join([str((x == i).sum()), i]) for i in set(x)]))
print('TXRX done')

# GET LNCEL
stmt = "SELECT (SELECT value FROM STRING_SPLIT (OBJ.[CO_DN], '/') WHERE value LIKE 'MR%') AS MRBTS, (SELECT value FROM STRING_SPLIT (OBJ.[CO_DN], '/') WHERE value LIKE 'LNCEL-%') AS LNCEL, LNCEL.LNCEL_CELL_NAME AS CellName, LNCEL_FDD.LNCEL_FDD_EARFCN_DL AS earfcnDL, CAST(LNCEL_FDD.LNCEL_FDD_DL_CH_BW/10 AS varchar) + 'MHz' AS dlChBw, LNCEL.LNCEL_P_MAX AS pMax, LNCEL_FDD.LNCEL_FDD_DL_MIMO_MODE as dlMimoMode FROM [dbo].[stage_T4OSS_C_LTE_LNCEL_FDD] AS LNCEL_FDD LEFT JOIN [dbo].[stage_T4OSS_CTP_COMMON_OBJECTS] AS OBJ ON LNCEL_FDD.OBJ_GID = OBJ.CO_GID LEFT JOIN [dbo].[stage_T4OSS_C_LTE_LNCEL] AS LNCEL ON OBJ.CO_PARENT_GID = LNCEL.OBJ_GID"
results = connection.execute(stmt).fetchall()
keys = results[0].keys()
df = pd.DataFrame(results)
df.columns = keys

df['Site'] = df.CellName.str[1:9]
df.dlMimoMode = df.dlMimoMode.map({0:'SingleTX', 10:'TXDiv', 11:'4-way TXDiv', 30:'Dynamic Open Loop MIMO', 40:'Closed Loop Mimo', 41:'Closed Loop MIMO (4x2)', 43:'Closed Loop MIMO (4x4)' })
df.MRBTS = df.MRBTS.str.replace('MRBTS-', '')
df.LNCEL = df.LNCEL.str.replace('LNCEL-', '')
df['CellID'] = df.MRBTS + '-' + df.LNCEL
df.LNCEL = df.LNCEL.astype(int)
df['Layer'] = pd.cut(df.LNCEL, range(0,111,10))
df['Layer'].cat.categories = ['L2100','L1900','L700','L2100-3','NA1','NA2','L600','NA3','NA4','NA5','L2100-2']
df.set_index('CellID', inplace=True)
print('LNCEL done')

#FINAL DF

result = pd.merge(pd.concat([df, TXRX], axis=1), pd.DataFrame(RMOD), on='MRBTS')
result = pd.merge(result, RFSH, on='MRBTS')

now = datetime.datetime.now()
result.to_csv(str(now).split()[0] + "_" + "CFG_QUERRY.csv")