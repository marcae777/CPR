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

    def logger(self,function,status,message):
        '''
        Logs methods performance
        Returns
        -------
        string comma separated values'''
        now = datetime.datetime.now().strftime(self.str_date_format)
        return '%s,%s,%s,%s'%(now,self.user,function,status,message)

    def read_sql(self,sql,close_db=True,*keys,**kwargs):
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
        conn_db = MySQLdb.connect(self.host,self.user,self.passwd,self.dbname)
        df = pd.read_sql(sql,conn_db,*keys,**kwargs)
        if close_db == True:
            conn_db.close()
        return df

    def execute_sql(self,query,close_db=True):
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
        conn_db = self.conn_db
        conn_db.cursor().execute(query)
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
            timer =  (datetime.datetime.now()-t).seconds/60.0
            format = (self.codigo,(count+1)*100.0/float(series.index.size),count+1,series.index.size,timer)

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
