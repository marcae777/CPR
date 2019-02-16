#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  CRP.py
#
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>
import pandas as pd
import numpy as np
import os
import datetime
import cpr_v1.information as info
from cpr_v1.SqlDb import SqlDb
import logging
from functools import wraps
import time
import math

class LevelSensor(SqlDb):
    ''' Provide functions to manipulate data related to a level sensor and its basin '''

    local_table  = 'meta_estaciones'
    remote_table = 'estaciones'

    def __init__(self,user,passwd,codigo = None,remote_server = info.REMOTE,**kwargs):
        '''
        The instance inherits modules to manipulate SQL
        Parameters
        ----------
        codigo        : primary key
        remote_server : keys to remote server
        local_server  : database kwargs to pass into the Sqldb class
        '''
        self.remote_server  = remote_server

        if not kwargs:
            kwargs = info.LOCAL
        SqlDb.__init__(self,codigo=codigo,user=user,passwd=passwd,**kwargs)

    def __repr__(self):
        '''string to recreate the object'''
        return "local_id: %s, Codigo: %s, Nombre: %s"%(self.info.id,self.info.codigo,self.info.nombre)

    def __str__(self):
        '''string to recreate the main information of the object'''
        return "local_id: %s, Codigo: %s, Nombre: %s"%(self.info.id,self.info.codigo,self.info.nombre)

    @property
    def info(self):
        '''
        Gets full information from current station
        Returns
        ---------
        pd.Series
        '''
        query = "SELECT * FROM %s WHERE clase = 'Nivel' and codigo='%s'"%(self.local_table,self.codigo)
        s = self.read_sql(query).T
        return s[s.columns[0]]

    @property
    def infost(self):
        '''
        Gets full information from all stations
        Returns
        ---------
        pd.DataFrame
        '''
        query = "SELECT * FROM %s WHERE clase ='Nivel'"%(self.local_table)
        return self.read_sql(query).set_index('codigo')

    def sensor(self,start,end,**kwargs):
        '''
        Reads remote sensor level data
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas time series
        '''
        sql = SqlDb(codigo = self.codigo,**self.remote_server)
        s = read_data_from_annoying_siata_date_format(self,['pr','NI'][self.info.tipo_sensor],start,end,**kwargs)
        return s

    def level(self,start=None,end=None,codigos=None,hours=3,local=False,**kwargs):
        '''
        Reads remote level data
        Parameters
        ----------
        start        : initial date
        end          : final date
        codigos      : stations to make dataframe with sensor data, if none, returns time Series
        Returns
        ----------
        pandas DataFrame with datetime index and basin radar fields
        '''
        if start:
            start = pd.to_datetime(start)
            end   = pd.to_datetime(end)
        else:
            end = self.round_time()
            start = end - datetime.timedelta(hours = hours)
        if local:
            if codigos:
                return self.level_all(start,end,codigos,local=local)
            else:
                format = (self.info.id,start.strftime(self.date_format),end.strftime(self.date_format))
                query = "SELECT fecha,profundidad FROM hydro_hydrodata where fk_id = '%s' and fecha between '%s' and '%s'"%format
                return self.read_sql(query).set_index('fecha')['profundidad']
        else:
            calidad = kwargs.get('calidad',True)
            if codigos:
                return self.level_all(start,end,codigos,calidad=calidad)
            else:
                s = self.sensor(start,end,calidad=calidad)
                serie = self.info.offset - s
                serie[serie>=self.info.offset_old] = np.NaN
                serie[serie<=0.0] = np.NaN
                return serie

    def level_all(self,start=None,end = None,codigos=None,hours=3,local=False,**kwargs):
        '''
        Reads level from several stations
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        if start:
            start = pd.to_datetime(start)
            end   = pd.to_datetime(end)
        else:
            end = self.round_time()
            start = end - datetime.timedelta(hours = hours)
        if codigos is None:
            codigos = self.infost.index
        if local:
            fields = 'meta_estaciones.codigo,hydro_hydrodata.fecha,hydro_hydrodata.profundidad'
            join = 'meta_estaciones ON hydro_hydrodata.fk_id = meta_estaciones.id'
            format = (fields,join,start.strftime(self.date_format),end.strftime(self.date_format))
            query = "SELECT %s FROM hydro_hydrodata INNER JOIN %s WHERE hydro_hydrodata.fecha between '%s' and '%s' "%format
            df = self.read_sql(query).set_index(['codigo','fecha']).unstack(0)['profundidad']
            return df[codigos]
        else:
            start = self.round_time(pd.to_dateti/me(start))
            df = pd.DataFrame(index = pd.date_range(start,end,freq='1min'),columns = codigos)
            for codigo in codigos:
                try:
                    level = Nivel(codigo=codigo,** info.LOCAL).level(start,end,**kwargs)
                    df[codigo] = level
                except:
                    pass
            return df

    def level_to_risk(self,value,risk_levels):
        ''' Convierte lamina de agua o profundidad a nivel de riesgo
        Parameters
        ----------
        value : float. Valor de profundidad o lamina de agua
        riskLevels: list,tuple. Niveles de riesgo

        Returns
        -------
        riskLevel : float. Nivel de riesgo
        '''
        if math.isnan(value):
            return np.NaN
        else:
            dif = value - np.array([0]+list(risk_levels))
            return int(np.argmin(dif[dif >= 0]))

    @property
    def risk_levels(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        query = "select n1,n2,n3,n4 from meta_estaciones where codigo = '%s'"%self.codigo
        return tuple(self.read_sql(query).values[0])


    def series_to_risk(self,level,risk_levels=None):
        '''
        Converts level series to risk
        Parameters
        ----------
        level   :   level sensor, pd.Series
        Returns
        ----------
        Risk levels, pd.Series
        '''
        risk = level.copy()
        colors = ['green','gold','orange','red','red','black']
        for codigo in level.index:
            try:
                if risk_levels is None:
                    risk[codigo] = colors[int(self.level_to_risk(level[codigo],self.risk_levels))]
                else:
                    risk[codigo] = colors[int(self.level_to_risk(level[codigo],risk_levels))]
            except:
                risk[codigo] = 'black'
        return risk

    def df_to_risk(self,df):
        '''converts level dataframe to risk_level dataframe
        Parameters
        ----------
        df  : level dataframe (obtainde usin self.level(start,end,codigos=[codigo1,codigo2,...]))
        Returns
        ----------
        Risk level DataFrame, pd.DataFrame"
        '''
        output = df.copy()
        for codigo in df:
            risk_levels = self.infost.loc[codigo,['n1','n2','n3','n4']].values
            output[codigo] = self.series_to_risk(df[codigo],risk_levels=risk_levels)
        return output

    def update_level_local(self,start,end):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        self.table = 'hydro_hydrodata'
        try:
            s = self.sensor(start,end).resample('5min').mean()
            self.update_series(s,'nivel')
        except:
            print ('WARNING: No data for %s'%self.codigo)

    def update_level_local_all(self,start,end):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        start,end = pd.to_datetime(start),pd.to_datetime(end)
        timer = datetime.datetime.now()
        size = self.infost.index.size
        for count,codigo in enumerate(self.infost.index):
            obj = Nivel(codigo = codigo,SimuBasin=False,**info.LOCAL)
            obj.table = 'hydro_hydrodata'
            obj.update_level_local(start,end)
        seconds = (datetime.datetime.now()-timer).seconds

    def calidad(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        end = datetime.datetime.now()
        start = end - datetime.timedelta(days=7)
        df = self.read_sql("select fecha,nivel,codigo from hydro_hydrodata where fecha between '%s' and '%s'"%(start.strftime('%Y-%m-%d %H:%M'),end.strftime('%Y-%m-%d %H:%M')))
        now = datetime.datetime.now()
        s = pd.DataFrame(df.loc[df.nivel.notnull()].groupby('codigo')['fecha'].max().sort_values())
        s['nombre'] = self.infost.loc[s.index,'nombre']
        s['delta'] = now-s['fecha']
        for horas,valor in zip([1,3,24,72],['green','yellow','orange','red']):
            r = s['fecha']<(now-datetime.timedelta(hours=horas))
            s.loc[s[r].index,'rango']=valor
        return s.dropna()

    def siata_remote_data_to_transfer(start,end,*args,**kwargs):
        '''
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
        parameters = tuple([codigos_str,self.fecha_hora_query(start,end)])
        df = remote.read_sql('SELECT * FROM datos WHERE cliente in %s and %s'%parameters)
        return df

    def data_to_transfer(self,start,end,local_path=None,remote_path=None,**kwargs):
        '''
        Gets pandas Series with data from tables with
        bad data
        Parameters
        ----------
        field        : Sql table field name
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas time Series
        '''
        transfer = self.siata_remote_data_to_transfer(start,end,**kwargs)
        def convert(x):
            try:
                value = pd.to_datetime(x).strftime('%Y-%m-%d')
            except:
                value = np.NaN
            return value
        transfer['fecha'] = transfer['fecha'].apply(lambda x:convert(x))
        transfer = transfer.loc[transfer['fecha'].dropna().index]
        if local_path:
            transfer.to_csv(local_path)
            if remote_path:
                os.system('scp %s %s'%(local_path,remote_path))
        return transfer
