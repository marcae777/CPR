import pandas as pd
import numpy as np
import os
import datetime
import cpr.information as info
from cpr.SqlDb import SqlDb
import logging
from functools import wraps
import time
import math
import MySQLdb

root = "root"
passwd = input('root password:')
self = SqlDb('cpr','root','localhost',passwd,3306)

if 'siata_Consulta' in self.read_sql("select user from mysql.user;")['user'].values:
    print('user already created')
else:
    statement = """CREATE USER 'siata_Consulta'@'localhost' IDENTIFIED BY 'si@t@64512_C0nsult4'"""
    self.execute_sql(statement)
    self.execute_sql("GRANT ALL PRIVILEGES ON siata.* TO 'siata_Consulta'@'localhost';")
    self.execute_sql("FLUSH PRIVILEGES")
try:
    self.execute_sql("CREATE DATABASE siata CHARACTER SET UTF8;")
except MySQLdb.ProgrammingError:
    print('Error: database exist')

self.execute_sql("ALTER TABLE  myusers_hydrodata ADD UNIQUE (fk_id,fecha);")    

self.dbname = 'siata'

for line in open('tablas_siata.sql'):
    try:
        self.execute_sql(line)
    except MySQLdb.OperationalError:
        pass

def siata_remote_data_to_transfer(start,end):
    remote = cpr.Nivel(**cpr.info.REMOTE)
    codigos_str = '('+str(list(self.infost.index)).strip('[]')+')'
    df = remote.read_sql('SELECT * FROM datos WHERE cliente in %s and %s'%(codigos_str,self.fecha_hora_query(start,end)))
    return df

def default_values(self,table_name):
    describe_table = self.read_sql('describe %s'%table_name)
    not_null = describe_table['Default'].notnull()
    default_values = describe_table[['Field','Default','Type']][not_null].set_index('Field')[['Default','Type']]
    return default_values

def filter_data_to_update(table_name,path):
    default = default_values(self,table_name)
    if default[default['Type']=='time'].empty:
        time_fields = None
    else:
        time_fields = default[default['Type']=='time']
    for index,s in time_fields.iterrows():
        df = pd.read_csv(path,index_col=0)
        df[index] = df[index].apply(lambda x:str(x)[6:15])
    return df.applymap(lambda x:str(x))

def insert_in_datos(path):
    inicia = datetime.datetime.now()
    table_name = 'datos'
    df = filter_data_to_update(table_name,path)
    query = 'INSERT INTO %s '%table_name+'('+str(list(df.columns)).strip('[]').replace("'",'')+') VALUES '
    for id,s in df.iterrows():
        query+=('('+str(list(s.values)).strip('[]'))+'), '
    query = query[:-2]+' ON DUPLICATE KEY UPDATE '
    describe = self.read_sql('describe %s;'%table_name)
    not_primary_keys = describe[describe['Key']!='PRI']
    if not_primary_keys.empty:
        not_primary_keys = describe.Field.values
    else:
        not_primary_keys = not_primary_keys.Field.values
    for key in not_primary_keys:
        query+=('%s = VALUES(%s), '%(key,key))
    query=query[:-2]
    finaliza = datetime.datetime.now()
    self.execute_sql('SET GLOBAL max_allowed_packet=1073741824;')
    self.execute_sql(query)
    print(finaliza - inicia)
    
def insert_myusers_hydrodata(self,df,field='profundidad'):
    '''
    Inserts data into myusers_hydrodata table, if fecha and fk_id exist, updates values.
    bad data
    Parameters
    ----------
    start        : initial date
    end          : final date
    field        : table field value to update
    Returns
    ----------
    pandas DataFrame
    '''
    df = df.copy()
    query = "INSERT INTO myusers_hydrodata (fk_id,fecha,%s,timestamp,updated,user_id) VALUES "%field
    df = df.unstack().reset_index()
    df.columns = ['fk_id','fecha',field]
    df[field]      = df[field]
    df['fk_id']    = self.infost.loc[np.array(df['fk_id'].values,int),'id'].values
    df[field]      = df[field].apply(lambda x:round(x,3))
    df = df.applymap(lambda x:str(x))
    df['timestap'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    df['updated']  = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
    df['user_id']  = '1'
    for id,s in df.iterrows():
        query+=('('+str(list(s.values)).strip('[]'))+'), '
    query = query[:-2].replace("'nan'",'NULL')
    query += ' ON DUPLICATE KEY UPDATE %s = VALUES(%s)'%(field,field)
    self.execute_sql(query)

folder_path = "/home/mcano/dev/backup/cprweb/src/media/weekly_data/"
files = os.listdir(folder_path)
print('Copying full history data into local mysql, it takes too much time. be patient!')
for path in files:
    insert_in_datos(folder_path+path)
     
self = Nivel(codigo=codigos[0],**info.LOCAL)
end = datetime.datetime.now()
start = end - datetime.timedelta(days = 30)
df = pd.read_csv('datos.csv',index_col=0)
df.index = pd.to_datetime(df.index)
N = 10 # number of times for split
index = np.array(np.linspace(0,df.index.size,N),int)
dfs = []
for i in range(0,len(index)):
    try:
        df_split = df.iloc[index[i]:index[i+1]].resample('5min').max()
        df_split.columns = np.array(df_split.columns,int)
        df_split.index = pd.to_datetime(df_split.index)
        insert_myusers_hydrodata(self,df_split,'profundidad')
    except IndexError:
        print('no worries dude!')
end = datetime.datetime.now()
start = end - datetime.timedelta(days = 30)
df = self.level_all(start,end)
