import pandas as pd
import os
import datetime
import cpr_v1.settings as info
import MySQLdb
import unittest

info.REMOTE = {
    'host'                      : "192.168.1.74",
    'user'                      :"siata_Consulta",
    'passwd'                    :"si@t@64512_C0nsult4",
    'table'                     : 'estaciones',
    'dbname'                    :"siata",
    'port'                      : 3306}

from cpr_v1.Level import Level
codigos = [93,99]

class TestCpr(unittest.TestCase):
	def test_algo(self):
		Level(codigo=codigos[0],**info.LOCAL)

self = Level(codigo=codigos[0],**info.LOCAL)
self.info.codigo == codigos[0]
self.infost.loc[codigos[0]].id == self.info.id
self.end = datetime.datetime.now()
self.start = self.end - datetime.timedelta(hours=1)
try:
    self.sensor().index.size >1
except MySQLdb.OperationalError:
    print('Error: No siata connection, install siata database-copy locally. Read Readme.md')
files = []
for file in os.listdir(info.DATA_PATH+'weekly_data/'):
    inicia = len('weekly_level_')
    finaliza = inicia + len('0000-00-00')
    files.append(file[inicia:finaliza])
files = pd.Series(index=pd.to_datetime(files)).sort_index()
self.host = 'localhost'
level = self.level()
level.index.size > 1
df = self.level(codigos)
df.columns.size==2
self.series_to_risk(level.fillna(0)*0).values[0] == 'green' # converts values to cero and tests it
self.series_to_risk(pd.Series(index=level.index)).values[0] == 'black' #  converts values to NaN and tests it
self.df_to_risk(df.fillna(0)*0)[codigos[0]].values[0] == 'green'
