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

def logger(orig_func):
    '''logging decorator, alters function passed as argument and creates
    log file. (contains function time execution)
    Parameters
    ----------
    orig_func : function to pass into decorator
    filepath  : file to save log file (ends with .log)
    Returns
    -------
    log file
    '''
    logging.basicConfig(logger.log,level=logging.INFO)
    @wraps(orig_func)
    def wrapper(*args,**kwargs):
        start = time.time()
        f = orig_func(*args,**kwargs)
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        took = time.time()-start
        log = '%s:%s:%.1f sec'%(date,orig_func.__name__,took)
        print(log)
        logging.info(log)
        return f
    return wrapper


def siata_remote_data_to_transfer(start,end):
    '''
    Creates DataFrame with table named datos from siata database
    Parameters
    ----------
    start        : initial date
    end          : final date
    Returns
    ----------
    pandas DataFrame
    '''
    remote = cpr.Nivel(**cpr.info.REMOTE)
    codigos_str = '('+str(list(self.infost.index)).strip('[]')+')'
    df = remote.read_sql('SELECT * FROM datos WHERE cliente in %s and %s'%(codigos_str,self.fecha_hora_query(start,end)))
    return df

def filter_data_to_update(table_name,path):
    '''
    Filters data before updating it
    Parameters
    ----------
    table_name    : Name of the table to update
    path          : file path to save .csv file
    Returns
    ----------
    pandas DataFrame
    '''
    def default_values(self,table_name):
        describe_table = self.read_sql('describe %s'%table_name)
        not_null = describe_table['Default'].notnull()
        default_values = describe_table[['Field','Default','Type']][not_null].set_index('Field')[['Default','Type']]
        return default_values

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
    '''
    Inserts data into hydro_hydrodata table, if fecha and fk_id exist, updates values.
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

def insert_hydro_hydrodata(self,df,field='profundidad'):
    '''
    Inserts data into hydro_hydrodata table, if fecha and fk_id exist, updates values.
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
    query = "INSERT INTO hydro_hydrodata (fk_id,fecha,%s,timestamp,updated,user_id) VALUES "%field
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

def read_data_from_annoying_siata_date_format(self,field,start,end,**kwargs):
    '''
    Gets pandas Series with data from tables with
    date format fecha and hora detached, and filters
    bad data
    Parameters
    ----------
    field        : Sql table field name
    start        : initial date
    end          : final date
    Kwargs
    ----------
    calidad      : True
    Returns
    ----------
    pandas time Series
    '''
    start  = pd.to_datetime(start).strftime('%Y-%m-%d %H:%M:00')
    end    = pd.to_datetime(end).strftime('%Y-%m-%d %H:%M:00')
    format = (field,self.codigo,self.fecha_hora_query(start,end))
    sql    = SqlDb(codigo = self.codigo,**info.REMOTE)
    if kwargs.get('calidad'):
        df = sql.read_sql("SELECT fecha,hora,%s from datos WHERE calidad = '1' and cliente = '%s' and %s"%format)
    else:
        df = sql.read_sql("SELECT fecha,hora,%s from datos WHERE cliente = '%s' and %s"%format)
    # converts centiseconds in 0
    try:
        df['hora']   = df['hora'].apply(lambda x:x[:-3]+':00')
    except TypeError:
        df['hora']   = df['hora'].apply(lambda x:str(x)[-8:-8+5]+':00')
        df['fecha']  = df['fecha'].apply(lambda x:x.strftime('%Y-%m-%d'))
    # concatenate fecha and hora fields, and makes nan bad datetime indexes
    df.index = pd.to_datetime(df['fecha'] + ' '+ df['hora'],errors='coerce')
    df = df.sort_index()
    # removes nan
    df = df.loc[df.index.dropna()]
    # masks duplicated index
    df[df.index.duplicated(keep=False)]=np.NaN
    df = df.dropna()
    # drops coluns fecha and hora
    df = df.drop(['fecha','hora'],axis=1)
    # reindex to have all indexes in full time series
    new_index = pd.date_range(start,end,freq='min')
    series = df.reindex(new_index)[field]
    return series

def insert_hydrodata(self,df,field='profundidad'):
    '''
    Inserts data into hydro_hydrodata table, if fecha and fk_id exist, updates values.
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
    query = "INSERT INTO hydro_hydrodata (fk_id,fecha,%s,timestamp,updated,user_id) VALUES "%field
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

def duplicate_table(self,table_name):
    '''
    inserts data into SQL table from list of fields and values
    Parameters
    ----------
    table_name   = SQL db table name
    Returns
    -------
    Sql sentence,str
    '''
    df = self.read_sql('describe %s'%table_name)
    df['Null'][df['Null']=='NO'] = 'NOT NULL'
    df['Null'][df['Null']=='YES'] = 'NULL'
    sentence = 'CREATE TABLE %s'%table_name
    if df[df['Extra']=='auto_increment'].empty:
        pk = None
    else:
        pk = df[df['Extra']=='auto_increment']['Field'].values[0]
    for id,serie in df.iterrows():
        if (serie.Default=='0') or (serie.Default is None):
            row = '%s %s %s'%(serie.Field,serie.Type,serie.Null)
        else:
            if (serie.Default == 'CURRENT_TIMESTAMP'):
                serie.Default = "DEFAULT %s"%serie.Default
            elif serie.Default == '0000-00-00':
                serie.Default = "DEFAULT '1000-01-01 00:00:00'"
            else:
                serie.Default = "DEFAULT '%s'"%serie.Default
            row = '%s %s %s %s'%(serie.Field,serie.Type,serie.Null,serie.Default)
        if serie.Extra:
            row += ' %s,'%serie.Extra
        else:
            row += ','
        sentence+=row
    if pk:
        sentence +='PRIMARY KEY (%s)'%pk
    else:
        sentence = sentence[:-1]
    sentence +=');'
    return sentence
