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

class Nivel(SqlDb):
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

    def convert_level_to_risk(self,value,risk_levels):
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
                    risk[codigo] = colors[int(self.convert_level_to_risk(level[codigo],self.risk_levels))]
                else:
                    risk[codigo] = colors[int(self.convert_level_to_risk(level[codigo],risk_levels))]
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
