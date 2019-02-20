import MySQLdb
import os
from cpr_v1.SqlDb import SiataDb
import cpr_v1.settings as info
from cpr_v1.Level import Level
import pandas as pd
import numpy as np
import datetime
import os

codigo = 93
local = Level(codigo=codigo,**info.LOCAL)
remote = SiataDb(codigo=codigo)
now = local.round_time(datetime.datetime.now())
remote.execute_sql("delete from datos")
df = pd.read_csv('sample_data_siata.csv',index_col=0)
offset = now - pd.to_datetime('2018-03-25 14:00:00')
fechas, horas = [],[]
for fecha,hora in zip(df['fecha'],df['hora']):
    fecha = pd.to_datetime(fecha)
    hora  = pd.to_timedelta(hora)
    fecha_hora = (fecha + hora)+offset
    fechas.append(fecha_hora.strftime('%Y-%m-%d'))
    horas.append(fecha_hora.strftime('%H:%M:00'))

df['fecha'] = fechas
df['hora'] = pd.to_timedelta(horas)
filepath = 'data_to_update.csv'
df.to_csv(filepath)
remote.execute_sql("delete from datos")
remote.insert_in_datos(filepath)
os.system('rm data_to_update.csv')
