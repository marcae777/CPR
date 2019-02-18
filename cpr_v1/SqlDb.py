#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  CRP.py
#
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>

import MySQLdb
import pandas as pd
import numpy as np
import datetime
from sqlalchemy import create_engine
import mysql.connector
import locale
import cpr_v1.settings as info
import logging
from functools import wraps
import time

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
    logging.basicConfig(filename = 'sql_log.log',level=logging.INFO)
    @wraps(orig_func)
    def wrapper(*args,**kwargs):
        start = time.time()
        f = orig_func(*args,**kwargs)
        date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        took = time.time()-start
        log = 'cpr_user:%s:%s:%s:%.1f sec'%(info.LOCAL['user'],date,orig_func.__name__,took)
        logging.info(log)
        return f
    return wrapper

class SqlDb:
    '''
    Class para manipular las bases de datos SQL
    '''
    date_format = '%Y-%m-%d %H:%M:00'

    def __init__(self,dbname,user,host,passwd,port,table=None,codigo=None,*keys,**kwargs):
        self.table  = table
        self.host   = host
        self.user   = user
        self.passwd = passwd
        self.dbname = dbname
        self.port   = port
        self.codigo = codigo

    def __repr__(self):
        '''string to recreate the object'''
        return "{} Obj".format(self.dbname)

    def __str__(self):
        '''string to recreate the main information of the object'''
        return "{} Obj".format(self.dbname)

    @property
    def conn_db(self):
        '''
        Engine connection: makes possible connection with SQL database
        '''
        conn_db = MySQLdb.connect(self.host,self.user,self.passwd,self.dbname,charset='utf8')
        return conn_db

    @logger
    def read_sql(self,statement,close_db=True,*keys,**kwargs):
        '''
        Read SQL query or database table into a DataFrame.
        Parameters
        ----------
        sql : string SQL query or SQLAlchemy Selectable (select or text object)
            to be executed, or database table name.

        keys and kwargs = ( sql, con, index_col=None, coerce_float=True,
                            params=None, parse_dates=None,columns=None,
                            chunksize=None)
        Returns
        -------
        DataFrame
        '''
        self.statement = statement
        conn_db = MySQLdb.connect(self.host,self.user,self.passwd,self.dbname)
        df = pd.read_sql(statement,conn_db,*keys,**kwargs)
        if close_db == True:
            conn_db.close()
        return df

    def execute_sql(self,statement,close_db=True):
        '''
        Execute SQL query or database table into a DataFrame.
        Parameters
        ----------
        query : string SQL query or SQLAlchemy Selectable (select or text object)
            to be executed, or database table name.
        keys = (sql, con, index_col=None, coerce_float=True, params=None,
        parse_dates=None,
        columns=None, chunksize=None)
        Returns
        -------
        DataFrame'''
        self.statement = statement
        conn_db = self.conn_db
        conn_db.cursor().execute(statement)
        conn_db.commit()
        if close_db == True:
            conn_db.close ()

    def insert_data(self,fields,values,*keys,**kwargs):
        '''
        inserts data into SQL table from list of fields and values
        Parameters
        ----------
        fields   = list of fields names from SQL db
        values   = list of values to be inserted
        Example
        -------
        insert_data(['fecha','nivel'],['2017-07-13',0.5])
        '''
        values = str(values).strip('[]')
        fields = str(fields).strip('[]').replace("'","")
        execution = 'INSERT INTO %s (%s) VALUES (%s)'%(self.table,fields,values)
        self.execute_sql(execution,*keys,**kwargs)

    def update_data(self,field,value,pk,*keys,**kwargs):
        '''
        Update data into SQL table
        Parameters
        ----------
        fields   = list of fields names from SQL db
        values   = list of values to be inserted
        pk       = primary key from table
        Example
        -------
        update_data(['nivel','prm'],[0.5,0.2],1025)
        '''
        query = "UPDATE %s SET %s = '%s' WHERE id = '%s'"%(self.table,field,value,pk)
        self.execute_sql(query,*keys,**kwargs)

    def df_to_sql(self,df,chunksize=20000,*keys,**kwargs):
        '''Replaces existing table with dataframe
        Parameters
        ----------
        df        = Pandas DataFrame to replace table
        chunksize = If not None, then rows will be written in batches
        of this size at a time
        '''
        format = (self.user,self.passwd,self.host,self.port,)
        engine = create_engine('mysql+mysqlconnector://%s:%s@%s:%s/cpr'%format,echo=False)
        df.to_sql(name      = self.table,
                  con       = engine,
                  if_exists = 'replace',
                  chunksize = chunksize,
                  index     = False,
                  *keys,**kwargs)

    def update_series(self,series,field):
        '''
        Update table from pandas time Series
        Parameters
        ----------
        series   = pandas time series with datetime or timestamp index
        and frequency = '5min'
        field    = field to be update
        Example
        value = series[fecha]
        ----------
        series = pd.Series(...,index=pd.date_range(...))
        update_series(series,'nivel')
        this updates the field nivel
        '''
        import math
        pk = self.id_df
        t  = datetime.datetime.now()
        for count,fecha in enumerate(series.index):
            value = series[fecha]
            if math.isnan(value):
                pass
            else:
                id    = pk[fecha]
                self.update_data(field,value,id)

    @staticmethod
    def round_time(date = datetime.datetime.now(),round_mins=5):
        '''
        Rounds datetime object to nearest x minutes
        Parameters
        ----------
        date         : date to round
        round_mins   : round to this nearest minutes interval
        Returns
        ----------
        datetime object rounded, datetime object
        '''
        mins = date.minute - (date.minute % round_mins)
        return datetime.datetime(date.year, date.month, date.day, date.hour, mins) + datetime.timedelta(minutes=round_mins)


    def duplicate_existing_table(self,table_name):
        '''
        Reads table properties and converts it into a create table statement
        for further insert into a different database
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
        sentence = 'CREATE TABLE %s '%table_name
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

class SiataDb(SqlDb):
    '''
    Class para manipular las bases de datos SQL
    '''
    def __init__(self,codigo=None,*keys,**kwargs):
        self.codigo = codigo
        SqlDb.__init__(self,codigo=codigo,**info.REMOTE)

    @staticmethod
    def fecha_hora_query(start,end):
        '''
        Efficient way to query in tables with fields fecha,hora
        such as table datos
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        Alternative query between two datetime objects
        '''
        start,end = pd.to_datetime(start),pd.to_datetime(end)
        def f(date):
            return tuple([date.strftime('%Y-%m-%d')]*2+[date.strftime('%H:%M:00')])
        query = "("+\
                "((fecha>'%s') or (fecha='%s' and hora>='%s'))"%f(start)+" and "+\
                "((fecha<'%s') or (fecha='%s' and hora<='%s'))"%f(end)+\
                ")"
        return query

    def read_fecha_hora_format(self,field,start,end,**kwargs):
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
        if kwargs.get('calidad'):
            df = self.read_sql("SELECT fecha,hora,%s from datos WHERE calidad = '1' and cliente = '%s' and %s"%format)
        else:
            df = self.read_sql("SELECT fecha,hora,%s from datos WHERE cliente = '%s' and %s"%format)
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

    def siata_remote_data_to_transfer(start,end,codigos=None,*args,**kwargs):
        '''
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas DataFrame
        '''
        if codigos is None:
            codigos = self.infost.index
        codigos_str = '('+str(list(codigos)).strip('[]')+')'
        parameters = tuple([codigos_str,self.fecha_hora_query(start,end)])
        df = self.read_sql('SELECT * FROM datos WHERE cliente in %s and %s'%parameters)
        def convert(x):
            try:
                value = pd.to_datetime(x).strftime('%Y-%m-%d')
            except:
                value = np.NaN
            return value
        df['fecha'] = df['fecha'].apply(lambda x:convert(x))
        df = df.loc[df['fecha'].dropna().index]
        return df


    def filter_data_to_update(self,table_name,path):
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

    def insert_in_datos(self,path):
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
        df = self.filter_data_to_update(table_name,path)
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

class HydroDb(SqlDb):
    '''
    Class para manipular las bases de datos SQL
    '''
    def __init__(self,codigo=None,*keys,**kwargs):
        self.codigo = codigo
        SqlDb.__init__(self,*keys,**kwargs)

    def insert_data_from_df(self,df,field='profundidad'):
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
        Sql execution
        '''
        df = df.copy()
        statement = "INSERT INTO hydro_hydrodata (fk_id,fecha,%s,timestamp,updated,user_id) VALUES "%field
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
            statement+=('('+str(list(s.values)).strip('[]'))+'), '
        statement = statement[:-2].replace("'nan'",'NULL')
        statement += ' ON DUPLICATE KEY UPDATE %s = VALUES(%s)'%(field,field)
        self.execute_sql(statement)
