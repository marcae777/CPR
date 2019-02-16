import pandas as pd
import numpy as np
import os
import datetime
import cpr.Information as info
from cpr.SqlDb import SqlDb
import logging
from functools import wraps
import time
import math
import MySQLdb
import unittest
from cpr.Nivel import Nivel

class TestHidraulica(unittest.TestCase):
	def test_algo(self):
		cpr.Nivel(codigo=93,**info.LOCAL)

info.REMOTE['host'] = '192.168.1.74'
codigos = [93,99]
self = Nivel(codigo=codigos[0],**info.LOCAL)
self.info.codigo == codigos[0]
self.infost.loc[codigos[0]].id == self.info.id
end = datetime.datetime.now()
start = end - datetime.timedelta(hours=1)
try:
    self.sensor(start,end).index.size >1
except MySQLdb.OperationalError:
    print('Error: No siata connection, install siata database-copy locally. Read Readme.md')
files = []
for file in os.listdir(info.DATA_PATH+'weekly_data/'):
    inicia = len('weekly_level_')
    finaliza = inicia + len('0000-00-00')
    files.append(file[inicia:finaliza])
files = pd.Series(index=pd.to_datetime(files)).sort_index()
end = datetime.datetime.now()
start = end - datetime.timedelta(hours=1)
self.host = 'localhost'
level = self.level(start,end)
level.index.size > 1
df = self.level(start,end,codigos)
df.columns.size==2
self.series_to_risk(level.fillna(0)*0).values[0] == 'green' # converts values to cero and tests it
self.series_to_risk(pd.Series(index=level.index)).values[0] == 'black' #  converts values to NaN and tests it
self.df_to_risk(df.fillna(0)*0)[codigos[0]].values[0] == 'green'
df = df.fillna(0)+666
insert_hydro_hydrodata(self,df.resample('5min').max())
objects_len = self.read_sql('select * from hydro_hydrodata').index.size
insert_hydrodata(self,df.resample('5min').max())
if objects_len==self.read_sql('select * from hydro_hydrodata').index.size:
    print('real')
else:
    print('insert data is duplicating objects, please add a constraint to the table in fk_id and fecha fields')
