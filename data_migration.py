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
local_filepath = info.DATA_PATH + 'data_migration/sample_data_remote.csv'
remote_filepath = info.DATA_PATH + 'data_migration/sample_data_local.csv'
now = local.round_time(datetime.datetime.now())

print(remote.execute_sql("delete from datos"))

for filepath in [local_filepath,remote_filepath]:
    df = pd.read_csv(filepath,index_col=0)
    df.index = pd.to_datetime(df.index)
    df.index = df.index + (now - (df.index[-1]-datetime.timedelta(hours=12)))
    df.to_csv(filepath)

remote.insert_in_datos(local_filepath)
df = pd.read_csv(remote_filepath,index_col=0)
df.index = pd.to_datetime(df.index)
df.index = df.index + (now - (df.index[-1]-datetime.timedelta(hours=12)))
N = 5
index = np.array(np.linspace(0,df.index.size,N),int)

for i in range(0,len(index)):
    try:
        df_split = df.iloc[index[i]:index[i+1]].resample('5min').max()
        df_split.columns = np.array(df_split.columns,int)
        df_split.index = pd.to_datetime(df_split.index)
        local.insert_data_from_df(df_split,'profundidad')
    except IndexError:
        pass
