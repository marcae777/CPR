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
import cpr.information as info
import matplotlib.colors as mcolors
from mpl_toolkits.basemap import Basemap
import matplotlib.pyplot as plt
from wmf import wmf
import matplotlib.dates as mdates
from reportlab.lib.pagesizes import letter
from reportlab.lib.units import inch
from matplotlib.patches import Polygon
from matplotlib.collections import PatchCollection
from cpr.SqlDb import SqlDb
from radar import radar

# default config
typColor = '#%02x%02x%02x' % (8,31,45)
plt.rc('axes',labelcolor=typColor)
plt.rc('axes',edgecolor=typColor)
plt.rc('text',color= typColor)
plt.rc('xtick',color=typColor)
plt.rc('ytick',color=typColor)


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
    import logging
    from functools import wraps
    import time
    logging.basicConfig(filename = info.DATA_PATH + 'logs/nivel.log',level=logging.INFO)
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

class Nivel(SqlDb,wmf.SimuBasin):
    ''' Provide functions to manipulate data related to a level sensor and its basin '''
    local_table  = 'estaciones_estaciones'
    remote_table = 'estaciones'
    def __init__(self,user,passwd,codigo = None,SimuBasin = False,remote_server = info.REMOTE,**kwargs):
        '''
        The instance inherits modules to manipulate SQL
        data and uses (hidrology modeling framework) wmf
        Parameters
        ----------
        codigo        : primary key
        remote_server : keys to remote server
        local_server  : database kwargs to pass into the Sqldb class
        nc_path       : path of the .nc file to set wmf class
        '''
        self.remote_server  = remote_server
        self.data_path      = info.DATA_PATH
        self.rain_path      = self.data_path + 'user_output/radar/'
        self.radar_path     = info.RADAR_PATH
        self.colores_siata  = [[0.69,0.87,0.93],[0.61,0.82,0.88],[0.32,0.71,0.77],[0.21,0.60,0.65],[0.0156,0.486,0.556],[0.007,0.32,0.36],[0.0078,0.227,0.26]]

        if not kwargs:
            kwargs = info.LOCAL
        SqlDb.__init__(self,codigo=codigo,user=user,passwd=passwd,**kwargs)

        if SimuBasin:
            query = "SELECT nc_path FROM %s WHERE codigo = '%s'"%(self.local_table,self.codigo)
            self.nc_path = self.data_path + 'basins/%s.nc'%self.codigo
            wmf.SimuBasin.__init__(self,rute=self.nc_path)

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

    @logger
    def get_radar_rain(self,start,end,cuenca,rutaNc,rutaRes,dt,umbral,verbose,super_verbose,old,save_class,save_escenarios,store_true,*args,**kwargs):
        '''
        Gets full information from all stations
        Returns
        ---------
        pd.Data
        '''
        from wmf import wmf
        import netCDF4
        import glob
        datesDias = pd.date_range(start,end,freq='D')
        a = pd.Series(np.zeros(len(datesDias)),index=datesDias)
        a = a.resample('A').sum()
        Anos = [i.strftime('%Y') for i in a.index.to_pydatetime()]
        datesDias = [d.strftime('%Y%m%d') for d in datesDias.to_pydatetime()]
        ListDays = []
        ListRutas = []
        for d in datesDias:
            try:
                L = glob.glob(rutaNc + d + '*.nc')
                ListRutas.extend(L)
                ListDays.extend([i[-23:-11] for i in L])
            except:
                pass
        #Organiza las listas de dias y de rutas
        ListDays.sort()
        ListRutas.sort()
        datesDias = []
        for d in ListDays:
            try:
                datesDias.append(datetime.datetime.strptime(d[:12],'%Y%m%d%H%M'))
            except:
                pass

        datesDias = pd.to_datetime(datesDias)
        #Obtiene las fechas por Dt
        textdt = '%d' % dt
        #Agrega hora a la fecha inicial
        datesDt = pd.date_range(start,end,freq = textdt+'s')
        #Obtiene las posiciones de acuerdo al dt para cada fecha
        PosDates = []
        pos1 = [0]
        for d1,d2 in zip(datesDt[:-1],datesDt[1:]):
                pos2 = np.where((datesDias<d2) & (datesDias>=d1))[0].tolist()
                if len(pos2) == 0:
                        pos2 = pos1
                else:
                        pos1 = pos2
                PosDates.append(pos2)
        cuAMVA = wmf.SimuBasin(rute = cuenca)
        cuConv = wmf.SimuBasin(rute = cuenca)
        cuStra = wmf.SimuBasin(rute = cuenca)
        cuHigh = wmf.SimuBasin(rute = cuenca)
        cuLow =  wmf.SimuBasin(rute = cuenca)

        #si el binario el viejo, establece las variables para actualizar
        if old:
            cuAMVA.rain_radar2basin_from_array(status='old',ruta_out= rutaRes)
            if save_class:
                cuConv.rain_radar2basin_from_array(status='old',ruta_out= rutaRes + '_conv')
                cuStra.rain_radar2basin_from_array(status='old',ruta_out= rutaRes + '_stra')
            if save_escenarios:
                cuHigh.rain_radar2basin_from_array(status='old',ruta_out= rutaRes + '_high')
                cuLow.rain_radar2basin_from_array(status='old',ruta_out= rutaRes + '_low')
        #Itera sobre las fechas para actualizar el binario de campos
        datesDt = datesDt.to_pydatetime()
        for dates,pos in zip(datesDt[1:],PosDates):
            rvec = np.zeros(cuAMVA.ncells, dtype = float)
            if save_escenarios:
                rhigh = np.zeros(cuAMVA.ncells, dtype = float)
                rlow = np.zeros(cuAMVA.ncells, dtype = float)
            Conv = np.zeros(cuAMVA.ncells, dtype = int)
            Stra = np.zeros(cuAMVA.ncells, dtype = int)
            try:
                for c,p in enumerate(pos):
                    #Lee la imagen de radar para esa fecha
                    g = netCDF4.Dataset(ListRutas[p])
                    RadProp = [g.ncols, g.nrows, g.xll, g.yll, g.dx, g.dx]
                    #Agrega la lluvia en el intervalo
                    rvec += cuAMVA.Transform_Map2Basin(g.variables['Rain'][:].T/ (12*1000.0), RadProp)
                    if save_escenarios:
                        rhigh += cuAMVA.Transform_Map2Basin(g.variables['Rhigh'][:].T / (12*1000.0), RadProp)
                        rlow += cuAMVA.Transform_Map2Basin(g.variables['Rlow'][:].T / (12*1000.0), RadProp)
                    #Agrega la clasificacion para la ultima imagen del intervalo
                    ConvStra = cuAMVA.Transform_Map2Basin(g.variables['Conv_Strat'][:].T, RadProp)
                    Conv = np.copy(ConvStra)
                    Conv[Conv == 1] = 0; Conv[Conv == 2] = 1
                    Stra = np.copy(ConvStra)
                    Stra[Stra == 2] = 0
                    rvec[(Conv == 0) & (Stra == 0)] = 0
                    if save_escenarios:
                        rhigh[(Conv == 0) & (Stra == 0)] = 0
                        rlow[(Conv == 0) & (Stra == 0)] = 0
                    Conv[rvec == 0] = 0
                    Stra[rvec == 0] = 0
                    #Cierra el netCDFs
                    g.close()
            except (Exception, e):
                rvec = np.zeros(cuAMVA.ncells)
                if save_escenarios:
                    rhigh = np.zeros(cuAMVA.ncells)
                    rlow = np.zeros(cuAMVA.ncells)
                Conv = np.zeros(cuAMVA.ncells)
                Stra = np.zeros(cuAMVA.ncells)
            dentro = cuAMVA.rain_radar2basin_from_array(vec = rvec,
                ruta_out = rutaRes,
                fecha = dates-datetime.timedelta(hours = 5),
                dt = dt,
                umbral = umbral)
            if save_escenarios:
                dentro = cuHigh.rain_radar2basin_from_array(vec = rhigh,
                    ruta_out = rutaRes+'_high',
                    fecha = dates-datetime.timedelta(hours = 5),
                    dt = dt,
                    umbral = umbral)
                dentro = cuLow.rain_radar2basin_from_array(vec = rlow,
                    ruta_out = rutaRes+'_low',
                    fecha = dates-datetime.timedelta(hours = 5),
                    dt = dt,
                    umbral = umbral)
            if dentro == 0:
                hagalo = True
            else:
                hagalo = False
            #mira si guarda o no los clasificados
            if save_class:
                #Escribe el binario convectivo
                aa = cuConv.rain_radar2basin_from_array(vec = Conv,
                    ruta_out = rutaRes+'_conv',
                    fecha = dates-datetime.timedelta(hours = 5),
                    dt = dt,
                    doit = hagalo)
                #Escribe el binario estratiforme
                aa = cuStra.rain_radar2basin_from_array(vec = Stra,
                    ruta_out = rutaRes+'_stra',
                    fecha = dates-datetime.timedelta(hours = 5),
                    dt = dt,
                    doit = hagalo)
            #Opcion Vervose
            if verbose:
                print (dates.strftime('%Y%m%d-%H:%M'), pos)
        #Cierrra el binario y escribe encabezado
        cuAMVA.rain_radar2basin_from_array(status = 'close',ruta_out = rutaRes)
        if save_class:
            cuConv.rain_radar2basin_from_array(status = 'close',ruta_out = rutaRes+'_conv')
            cuStra.rain_radar2basin_from_array(status = 'close',ruta_out = rutaRes+'_stra')
        if save_escenarios:
            cuHigh.rain_radar2basin_from_array(status = 'close',ruta_out = rutaRes+'_high')
            cuLow.rain_radar2basin_from_array(status = 'close',ruta_out = rutaRes+'_low')
        #Imprime en lo que va
        if verbose:
                print ('Encabezados de binarios de cuenca cerrados y listos')

    @staticmethod
    def hdr_to_series(path):
        '''
        Reads hdr rain files and converts it into pandas Series
        Parameters
        ----------
        path         : path to .hdr file
        Returns
        ----------
        pandas time Series with mean radar rain
        '''
        s =  pd.read_csv(path,skiprows=5,usecols=[2,3]).set_index(' Fecha ')[' Lluvia']
        s.index = pd.to_datetime(s.index)
        return s

    @staticmethod
    def hdr_to_df(path):
        '''
        Reads hdr rain files and converts it into pandas DataFrame
        Parameters
        ----------
        path         : path to .hdr file
        Returns
        ----------
        pandas DataFrame with mean radar rain
        '''
        if path.endswith('.hdr') != True:
            path = path+'.hdr'
        df = pd.read_csv(path,skiprows=5).set_index(' Fecha ')
        df.index = pd.to_datetime(df.index)
        df = df.drop('IDfecha',axis=1)
        df.columns = ['record','mean_rain']
        return df

    def bin_to_df(self,path,start=None,end=None,**kwargs):
        '''
        Reads rain fields (.bin) and converts it into pandas DataFrame
        Parameters
        ----------
        path         : path to .hdr and .bin file
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas DataFrame with mean radar rain
        Note
        ----------
        path without extension, ejm folder_path/file not folder_path/file.bin,
        if start and end is None, the program process all the data
        '''
        start,end = pd.to_datetime(start),pd.to_datetime(end)
        df = self.hdr_to_df(path)
        if (start is not None) and (end is not None):
            df = df.loc[start:end]
        df = df[df['record']!=1]
        records = df['record'].values
        rain_field = []
        for count,record in enumerate(records):
            rain_field.append(wmf.models.read_int_basin('%s.bin'%path,record,self.ncells)[0])
            count = count+1
            format = (count*100.0/len(records),count,len(records))
        return pd.DataFrame(np.matrix(rain_field),index=df.index)

    def file_format(self,start,end):
        '''
        Returns the file format customized for elements containing
        starting and ending point
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        file format with datetimes like %Y%m%d%H%M
        Example
        ----------
        '''
        start,end = pd.to_datetime(start),pd.to_datetime(end)
        format = '%Y%m%d%H%M'
        return '%s-%s-%s-%s'%(start.strftime(format),end.strftime(format),self.codigo,self.user)

    def file_format_date_to_datetime(self,string):
        '''
        Transforms string in file_format like string to datetime object
        Parameters
        ----------
        string         : string object in file_format like time object
        Returns
        ----------
        datetime object
        Example
        ----------
        In : self.file_format_date_to_datetime('201707141212')
        Out: Timestamp('2017-07-14 12:12:00')
        '''
        format = (string[:4],string[4:6],string[6:8],string[8:10],string[10:12])
        return pd.to_datetime("%s-%s-%s %s:%s"%format)

    def file_format_to_variables(self,string):
        '''
        Splits file name string in user and datetime objects
        Parameters
        ----------
        string         : file name
        Returns
        ----------
        (user,start,end) - (string,datetime object,datetime object)
        '''
        string = string[:string.find('.')]
        start,end,codigo,user = list(x.strip() for x in string.split('-'))
        start,end = self.file_format_date_to_datetime(start),self.file_format_date_to_datetime(end)
        return start,end,int(codigo),user

    def check_rain_files(self,start,end):
        '''
        Finds out if rain data has already been processed
        start        : initial date
        end          : final date
        Returns
        ----------
        file path or None for no coincidences
        '''
        def todate(date):
            return pd.to_datetime(pd.to_datetime(date).strftime('%Y-%m-%d %H:%M'))
        start,end = todate(start),todate(end)
        files = os.listdir(self.rain_path)
        if files:
            for file in files:
                try:
                    comienza,finaliza,codigo,usuario = self.file_format_to_variables(file)
                    if (comienza<=start) and (finaliza>=end) and (codigo==self.codigo):
                        ext = file[file.find('.'):]
                        file =  file[:file.find('.')]
                        if (file+'.bin' in files) and (file+'.hdr' in files):
                            break
                        else:
                            pt = self.rain_path + file + ext
                            os.remove(str(pt))
                            file = None
                    else:
                        file = None
                except:
                    file = None
        else:
            file = None
        return file

    def radar_rain(self,start,end,ext='.hdr'):
        '''
        Reads rain fields (.bin or .hdr)
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas DataFrame or Series with mean radar rain
        '''
        start,end = pd.to_datetime(start),pd.to_datetime(end)
        file = self.check_rain_files(start,end)
        if file:
            file = self.rain_path+file
            if ext == '.hdr':
                obj =  self.hdr_to_series(file+'.hdr')
            else:
                obj =  self.bin_to_df(file)
            obj = obj.loc[start:end]
        else:
            print ('WARNING: converting rain data, it may take a while')
            delay = datetime.timedelta(hours=5)
            kwargs =  {
                        'start':start+delay,
                        'end':end+delay,
                        'cuenca':self.nc_path,
                        'rutaNc':self.radar_path,
                        'rutaRes':self.rain_path+self.file_format(start,end),
                        'dt':300,
                        'umbral': 0.005,
                        'verbose':False,
                        'super_verbose':True,
                        'old':None,
                        'save_class':None,
                        'store_true':None,
                        'save_escenarios':None,
                        'store_true':None,
                       }
            self.get_radar_rain(**kwargs)
            file = self.rain_path + self.check_rain_files(start,end)
            if ext == '.hdr':
                obj =  self.hdr_to_series(file+'.hdr')
            else:
                obj =  self.bin_to_df(file)
            obj = obj.loc[start:end]
        return obj

    def radar_rain_vect(self,start,end,**kwargs):
        '''
        Reads rain fields (.bin)
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas DataFrame with datetime index and basin radar fields
        '''
        return self.radar_rain(start,end,ext='.bin',**kwargs)

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
        s = self.fecha_hora_format_data(['pr','NI'][self.info.tipo_sensor],start,end,**kwargs)
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
                return self.level_all(start,end,codigos=codigos,local=local)
            else:
                format = (self.info.id,start.strftime(self.date_format),end.strftime(self.date_format))
                query = "SELECT fecha,profundidad FROM myusers_hydrodata where fk_id = '%s' and fecha between '%s' and '%s'"%format
                return self.read_sql(query).set_index('fecha')['profundidad']
        else:
            calidad = kwargs.get('calidad',True)
            if codigos:
                return self.level_all(start,end,codigos=codigos,calidad=calidad)
            else:
                s = self.sensor(start,end,calidad=calidad)
                serie = self.info.offset - s
                serie[serie>=self.info.offset_old] = np.NaN
                serie[serie<=0.0] = np.NaN
                return serie

    def offset_remote(self):
        '''
        Reads remote offset
        Parameters
        ----------
        Returns
        ----------
        remote offset, Float
        '''
        remote = SqlDb(**self.remote_server)
        query = "SELECT codigo,fecha_hora,offset FROM historico_bancallena_offset"
        df = remote.read_sql(query).set_index('codigo')
        try:
            offset = float(df.loc[self.codigo,'offset'])
        except TypeError:
            offset =  df.loc[self.codigo,['fecha_hora','offset']].set_index('fecha_hora').sort_index()['offset'][-1]
        return offset


    def mysql_query(self,query,pandas=True):
        '''
        Old sql way to get data, if pandas = False, returns a matrix
        Parameters
        ----------
        query   :   data base query
        '''
        conn_db = MySQLdb.connect(self.host, self.user, self.passwd, self.dbname)
        db_cursor = conn_db.cursor ()
        db_cursor.execute (query)
        if toPandas == True:
            data = pd.DataFrame(np.matrix(db_cursor.fetchall()))
        else:
            data = db_cursor.fetchall()
        conn_db.close()
        return data

    def last_topo(self):
        fk_id = self.read_sql("SELECT id,fecha from myusers_item where item_fk_id = '%s'"%self.info.id).sort_values('fecha').loc[0,'id']
        last_bat = self.read_sql('SELECT * FROM myusers_topo where fk_id = "%s"'%fk_id)
        return last_bat

    def get_sections(self,levantamiento,level):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        hline = ((levantamiento['x'].min()*1.1,level),(levantamiento['x'].max()*1.1,level)) # horizontal line
        lev = pd.DataFrame.copy(levantamiento) #df to modify
        #PROBLEMAS EN LOS BORDES
        borderWarning = 'Warning:\nProblemas de borde en el levantamiento'
        if lev.iloc[0]['y']<level:
            lev = pd.DataFrame(np.matrix([lev.iloc[0]['x'],level]),columns=['x','y']).append(lev)
        if lev.iloc[-1]['y']<level:
            lev = lev.append(pd.DataFrame(np.matrix([lev.iloc[-1]['x'],level]),columns=['x','y']))
        condition = (lev['y']>=level).values
        flag = condition[0]
        nlev = []
        intCount = 0
        ids=[]
        for i,j in enumerate(condition):
            if j==flag:
                ids.append(i)
                nlev.append(list(lev.iloc[i].values))
            else:
                intCount+=1
                ids.append('Point %s'%intCount)
                line = (list(lev.iloc[i-1].values),list(lev.iloc[i].values)) #  #puntoA
                inter = self.line_intersection(line,hline)
                nlev.append(inter)
                ids.append(i)
                nlev.append(list(lev.iloc[i].values))
            flag = j
        df = pd.DataFrame(np.matrix(nlev),columns=['x','y'],index=ids)
        dfs = []
        for i in np.arange(1,100,2)[:int(intCount/2)]:
            dfs.append(df.loc['Point %s'%i:'Point %s'%(i+1)])
        return dfs

    @staticmethod
    def get_area(x,y):
        '''Calcula las áreas y los caudales de cada
        una de las verticales, con el método de mid-section
        Input:
        x = Distancia desde la banca izquierda, type = numpy array
        y = Produndidad
        Output:
        area = Área de la subsección
        Q = Caudal de la subsección
        '''
        # cálculo de áreas
        d = np.absolute(np.diff(x))/2.
        b = x[:-1]+ d
        area = np.diff(b)*y[1:-1]
        area = np.insert(area, 0, d[0]*y[0])
        area = np.append(area,d[-1]*y[-1])
        area = np.absolute(area)
        # cálculo de caudal
        return area

    def get_areas(self,dfs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        area = 0
        for df in dfs:
            area+=sum(self.get_area(df['x'].values,df['y'].values))
        return area

    @staticmethod
    def line_intersection(line1, line2):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        xdiff = (line1[0][0] - line1[1][0], line2[0][0] - line2[1][0])
        ydiff = (line1[0][1] - line1[1][1], line2[0][1] - line2[1][1])
        def det(a, b):
            return a[0] * b[1] - a[1] * b[0]

        div = det(xdiff, ydiff)
        if div == 0:
           raise Exception('lines do not intersect')
        d = (det(*line1), det(*line2))
        x = det(d, xdiff) / div
        y = det(d, ydiff) / div
        return (x, y)

    def longitude_latitude_basin(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        mcols,mrows = wmf.cu.basin_2map_find(self.structure,self.ncells)
        mapa,mxll,myll=wmf.cu.basin_2map(self.structure,self.structure[0],mcols,mrows,self.ncells)
        longs = np.array([mxll+0.5*wmf.cu.dx+i*wmf.cu.dx for i in range(mcols)])
        lats  = np.array([myll+0.5*wmf.cu.dx+i*wmf.cu.dx for i in range(mrows)])
        return longs,lats

    def basin_mappable(self,vec=None, extra_long=0,extra_lat=0,perimeter_keys={},contour_keys={},**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        longs,lats=self.longitude_latitude_basin()
        x,y=np.meshgrid(longs,lats)
        y=y[::-1]
        # map settings
        m = Basemap(projection='merc',llcrnrlat=lats.min()-extra_lat, urcrnrlat=lats.max()+extra_lat,
            llcrnrlon=longs.min()-extra_long, urcrnrlon=longs.max()+extra_long, resolution='c',**kwargs)
        # perimeter plot
        xp,yp = m(self.Polygon[0], self.Polygon[1])
        m.plot(xp, yp,**perimeter_keys)
        # vector plot
        if vec is not None:
            map_vec,mxll,myll=wmf.cu.basin_2map(self.structure,vec,len(longs),len(lats),self.ncells)
            map_vec[map_vec==wmf.cu.nodata]=np.nan
            xm,ym=m(x,y)
            contour = m.contourf(xm, ym, map_vec.T, 25,**contour_keys)
        else:
            contour = None
        return m,contour

    def adjust_basin(self,rel=0.766,fac=0.0):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        longs,lats = self.longitude_latitude_basin()
        x = longs[-1]-longs[0]
        y = lats[-1] - lats[0]
        if x>y:
            extra_long = 0
            extra_lat = (rel*x-y)/2.0
        else:
            extra_lat=0
            extra_long = (y/(2.0*rel))-(x/2.0)
        return extra_lat+fac,extra_long+fac


    def radar_cmap(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        import matplotlib.colors as colors
        bar_colors=[(255, 255, 255),(0, 255, 255), (0, 0, 255),(70, 220, 45),(44, 141, 29),(255,255,75),(255,142,0),(255,0,0),(128,0,128),(102,0,102),(255, 153, 255)]
        lev = np.array([0.,1.,5.,10.,20.,30.,45.,60., 80., 100., 150.])
        scale_factor =  ((255-0.)/(lev.max() - lev.min()))
        new_Limits = list(np.array(np.round((lev-lev.min())*\
                                    scale_factor/255.,3),dtype = float))
        Custom_Color = list(map(lambda x: tuple(ti/255. for ti in x) , bar_colors))
        nueva_tupla = [((new_Limits[i]),Custom_Color[i],) for i in range(len(Custom_Color))]
        cmap_radar =colors.LinearSegmentedColormap.from_list('RADAR',nueva_tupla)
        levels_nuevos = np.linspace(np.min(lev),np.max(lev),255)
        norm_new_radar = colors.BoundaryNorm(boundaries=levels_nuevos, ncolors=256)
        return cmap_radar,levels_nuevos,norm_new_radar

    def level_local(self,start,end,offset='new'):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        if offset=='new':
            offset = self.info.offset
        else:
            offset = self.info.offset_old
        format = (self.codigo,start,end)
        query = "select fecha,profundidad from myusers_hydrodata where codigo='%s' and fecha between '%s' and '%s';"%format
        level =  (offset - self.read_sql(query).set_index('fecha')['nivel'])
        level[level>self.risk_levels[-1]*1.2] = np.NaN
        level[level>offset] = np.NaN
        return level

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
        import math
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
        query = "select n1,n2,n3,n4 from estaciones_estaciones where codigo = '%s'"%self.codigo
        return tuple(self.read_sql(query).values[0])

    def risk_level_series(self,start,end):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        return self.level_local(start,end).apply(lambda x: self.convert_level_to_risk(x,self.risk_levels))

    def risk_level_df(self,start,end):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        df = pd.DataFrame(index=pd.date_range(start,end,freq='D'),columns=self.infost.index)
        for count,codigo in enumerate(df.columns):
            try:
                clase = Nivel(user=self.user,codigo=codigo,passwd=self.passwd,**info.LOCAL)
                df[codigo] = clase.risk_level_series(start,end).resample('D',how='max')
            except:
                df[codigo] = np.NaN
        return df

    def plot_basin_rain(self,vec,cbar=None,ax=None,**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        if ax is None:
            fig = plt.figure(figsize=(10,16))
            ax = fig.add_subplot()
        cmap_radar,levels,norm = self.radar_cmap()
        extra_lat,extra_long = self.adjust_basin(fac=0.01)
        mapa,contour = self.basin_mappable(vec,
                                      ax=ax,
                                      extra_long=extra_long,
                                      extra_lat = extra_lat,
                                      contour_keys={'cmap'  :cmap_radar,
                                                    'levels':levels,
                                                    'norm'  :norm},
                                     perimeter_keys={'color':'k'})
        if cbar:
            cbar = mapa.colorbar(contour,location='right',pad="15%")
            cbar.ax.set_title('mm',fontsize=14)
        else:
            cbar = mapa.colorbar(contour,location='right',pad="15%")
            cbar.remove()
            plt.draw()
        net_path = kwargs.get('net_path',self.data_path + "shapes/net/%s/%s"%(self.codigo,self.codigo))
        stream_path = kwargs.get('stream_path',self.data_path + "shapes/stream/%s/%s"%(self.codigo,self.codigo))
        mapa.readshapefile(net_path,'net_path')
        mapa.readshapefile(stream_path,'stream_path',linewidth=1)
        return mapa

    def plot_section(self,df,*args,**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        # Kwargs
        level = kwargs.get('level',None)
        xLabel = kwargs.get('xLabel','Distancia desde la margen izquierda [m]')
        yLabel = kwargs.get('yLabel','Profundidad [m]')
        waterColor = kwargs.get('waterColor',self.colores_siata[1])
        groundColor = kwargs.get('groundColor','#%02x%02x%02x' % (8,31,45))
        fontsize= kwargs.get('fontsize',14)
        figsize = kwargs.get('figsize',(10,4))
        riskLevels = kwargs.get('riskLevels',None)
        xSensor = kwargs.get('xSensor',None)
        offset = kwargs.get('offset',self.info.offset)
        scatterSize = kwargs.get('scatterSize',0.0)
        ax = kwargs.get('ax',None)
        # main plot
        if ax is None:
            fig = plt.figure(figsize=figsize)
            ax = fig.add_subplot(111)
        ax.plot(df['x'].values,df['y'].values,color='k',lw=0.5)
        ax.fill_between(np.array(df['x'].values,float),np.array(df['y'].values,float),float(df['y'].min()),color=groundColor,alpha=0.2)
        # waterLevel
        sections = []
        if level is not None:
            for data in self.get_sections(df,level):
                #ax.hlines(level,data['x'][0],data['x'][-1],color='k',linewidth=0.5)
                ax.fill_between(data['x'],level,data['y'],color=waterColor,alpha=0.9)
                sections.append(data)

        if (offset is not None) and (xSensor is not None):
            ax.scatter(xSensor,level,marker='v',color='k',s=30+scatterSize,zorder=22)
            ax.scatter(xSensor,level,color='white',s=120+scatterSize+10,edgecolors='k')

        ax.set_xlabel(xLabel)
        ax.set_facecolor('white')
        xlim_max = df['x'].max()
        if riskLevels is not None:
            x = df['x'].max() -df['x'].min()
            y = df['y'].max() -df['y'].min()
            factorx = 0.05
            ancho = x*factorx
            locx = df['x'].max()+ancho/2.0
            miny = df['y'].min()
            locx = 1.03*locx
            risks = np.diff(np.array(list(riskLevels)+[offset]))
            ax.bar(locx,[riskLevels[0]+abs(miny)],width=ancho,bottom=0,color='green')
            colors = ['yellow','orange','red','red']
            for i,risk in enumerate(risks):
                ax.bar(locx,[risk],width=ancho,bottom=riskLevels[i],color=colors[i],zorder=19)

            if level is not None:
                ax.hlines(data['y'].max(),data['x'].max(),locx,lw=1,linestyles='--')
                ax.scatter([locx],[data['y'].max()],s=30,color='k',zorder=20)
            xlim_max=locx+ancho
#        ax.hlines(data['y'].max(),df['x'].min(),sections[0].min(),lw=1,linestyles='--')
        ax.set_xlim(df['x'].min(),xlim_max)

    def in_risk(self,start,end):
        risk = self.risk_level_df(start,end)
        return risk.sum()[risk.sum()!=0.0].index

    @property
    def id_df(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        return self.read_sql("select fecha,id from id_hydro where codigo = '%s'"%self.codigo).set_index('fecha')['id']

    def gif_level(self,start,end,delay = 30,loop=0,**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        path = kwargs.get('path',self.data_path)
        level = self.level_local(start,end)
        os.system('rm -r %s/*.png'%path)
        for count in range(level.index.size):
            try:
                nivel = level.copy()
                nivel[count:] = np.NaN
                self.plot_level(nivel,
                                nivel.dropna()[-1]/100.0,
                                figsize=(12,3))
                plt.savefig('%s/%.3d.png'%(path,count),
                            bbox_inches='tight')
                plt.close()
            except:
                pass
        file_name = self.file_format(start,end)+'-gif'
        query = "convert -delay %s -loop %s %s/*.png %s/%s.gif"%(delay,loop,path,path,file_name)
        r = os.system(query)
        if r ==0:
            print('INFO: gif saved in path: %s/%s'%(path,file_name))
        else:
            print('ERROR: gif not saved')

    def plot_level(self,series,lamina='current',resolution='m',legend=True,ax=None,scatter=True,**kwargs):
        '''
        Parameters
        ----------
        series      : level time series pd.Series
        resolution  : list,tuple. Niveles de riesgo
        legend      : risk level legend
        ax          : axis
        scatter     : show level icon, bool
        kwargs : figsize,scatter_size,scatter_color,risk_levels,ymax
        '''
        figsize       = kwargs.get('figsize',(12,4))
        scatter_size  = kwargs.get('scatter_size',0)
        scatter_color = kwargs.get('scatter_color','k')
        risk_levels   = kwargs.get('risk_levels',np.array(self.risk_levels,float)/100.0)
        ymax          = kwargs.get('ymax',max(risk_levels)*1.05)
        if ax is None:
            fig = plt.figure(figsize=figsize)
            ax = fig.add_subplot(111)
        # plot
        ax.fill_between(series.index,series.values,color=self.colores_siata[1],label='Open values')
        # water level
        if series.dropna().index.size == 0:
            x,lamina = (series.index[-1],0.0)
        else:
            if type(lamina) == str:
                if lamina == 'current':
                    x,lamina = (series.dropna().index[-1],series.dropna().iloc[-1])
                else:
                    x,lamina = (series.argmax(),series.max())
        # scatter
        if series.dropna().index.size == 0:
            pass
        else:
            if scatter:
                ax.scatter(x,lamina,marker='v',color=scatter_color,s=30+scatter_size,zorder=22)
                ax.scatter(x,lamina,color='white',s=120+scatter_size+10,edgecolors=scatter_color)
            else:
                pass
        ax.spines['top'].set_color('w')
        ax.spines['right'].set_color('w')
        ax.set_ylabel('Profundidad [%s]'%resolution)
        # legend
        if legend:
            bounds = [(self.loc_time_series(series,0.03)),(self.loc_time_series(series,0.08))]
            ax.axvspan(bounds[0],
                       bounds[1],
                       ymin=0.0,
                       ymax=risk_levels[0]/ymax,
                       color='green')

            ax.axvspan(bounds[0],
                       bounds[1],
                       ymin=risk_levels[0]/ymax,
                       ymax=risk_levels[1]/ymax,
                       color='yellow')

            ax.axvspan(bounds[0],
                       bounds[1],
                       ymin=risk_levels[1]/ymax,
                       ymax=risk_levels[2]/ymax,
                       color='orange')

            ax.axvspan(bounds[0],
                       bounds[1],
                       ymin=risk_levels[2]/ymax,
                       ymax=ymax,
                       color='red')
            ax.set_xlim(series.index[0],bounds[1])
        ax.set_ylim(0,ymax)
        return ax

    def loc_time_series(self,series,percent):
        return series.index[-1]+(series.index[-1]-series.index[0])*percent

    def plot_operacional(self,series,bat,window,filepath):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        font = {'size'   :25}
        plt.rc('font', **font)
        # figure
        fig  = plt.figure(figsize=(16,24))
        fig.subplots_adjust(hspace=0.8)
        ax = fig.add_subplot(311)
        ax.set_title('Serie de tiempo')
        max_text=True
        if window == '3h':
            lamina = 'current'
        else:
            lamina = 'max'
        try:
            if len(series.dropna()>=1):
                self.plot_level(series,
                                lamina=lamina,
                                risk_levels=np.array(self.risk_levels)/100.0,
                                resolution='m',
                                ax=ax,
                                scatter_size=40)
            else:
                now=pd.datetime.now()
                new_time=pd.date_range(now-datetime.timedelta(hours=int(window.split('h')[0])),now,freq='10T')
                series_aux=pd.Series(np.zeros(len(new_time)),index=new_time)

                self.plot_level(series_aux,
                                lamina=lamina,
                                risk_levels=np.array(self.risk_levels)/100.0,
                                resolution='m',
                                ax=ax,
                                scatter_size=40)
                max_text=False
                ax.set_title('Datos no disponibles para la ventana de tiempo')


            for tick in ax.xaxis.get_major_ticks():
                tick.set_pad( 5.5 * tick.get_pad() )
            # subaxis for xticks in siata format
            box = ax.get_position() #mirror
            subax = fig.add_axes([box.min[0],box.min[1]*0.95, box.width,box.height])
            subax.patch.set_alpha(0.0)
            subs = pd.Series(index=series.index)
            subax.plot(series.index,pd.Series(index=series.index).values,color='w')
            for loc in ['top','right','left']:
                subax.spines[loc].set_visible(False)
            subax.set_xlim(ax.get_xlim())
            # date locator
            major_locator_format = '%d %b %y'
            if window == '3h':
                minor_locator        = mdates.HourLocator(interval=1)
                major_locator        = mdates.DayLocator(interval=1)
            elif window == '24h':
                minor_locator        = mdates.HourLocator(interval=6)
                major_locator        = mdates.DayLocator(interval=1)
            elif window == '72h':
                minor_locator        = mdates.DayLocator(interval=1)
                major_locator        = mdates.DayLocator(interval=1)
            else:
                minor_locator        = mdates.DayLocator(interval=7)
                major_locator        = mdates.DayLocator(interval=7)
            if window !='3h':
                if max_text:
                    ax.annotate(u'máximo', (mdates.date2num(series.argmax()), series.max()), xytext=(10, 10),textcoords='offset points',fontsize=14)
            ax.xaxis.set_major_locator(minor_locator) # notice minor_locator is the max_locator in ax
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
            subax.xaxis.set_major_locator(major_locator)
            subax.xaxis.set_major_formatter(mdates.DateFormatter(major_locator_format))
            subax.yaxis.set_major_locator(plt.NullLocator())
        except:
            pass

        # section
        ax2 = fig.add_subplot(312)
        alpha=0.2
        ymax = max([bat['y'].max(),(self.risk_levels[-1])/100.0])

            # plot section
        if series.dropna().index.size == 0:
            lamina = 0.0
        else:
            if lamina == 'current':
                x,lamina = (series.dropna().index[-1],series.dropna().iloc[-1])
            else:
                x,lamina = (series.argmax(),series.max())
        try:
            sections =self.plot_section(bat,
                                    ax = ax2,
                                    level=lamina,
                                    riskLevels=np.array(self.risk_levels)/100.0,
                                    xSensor=self.info.x_sensor,
                                    scatterSize=50)
        except:
            pass
        ax2.spines['top'].set_color('w')
        ax2.spines['right'].set_color('w')
        ax2.spines['right'].set_color('w')
        ax2.set_ylim(bat['y'].min(),ymax)
        ax2.set_title('Profundidad en el canal')
        ax2.set_ylabel('Profundidad [m]')
        # ax3 RESUMEN
        try:
            format =   (['ultrasonido','radar'][self.info.tipo_sensor],
                        series.dropna().index.size*100.0/series.index.size,
                        series.max(),
                        ['verde','amarillo','naranja','rojo','rojo'][self.convert_level_to_risk(series.max(),np.array(self.risk_levels)/100.0)],
                        series.mean())
            text= u'-Estación de Nivel tipo %s\n-Resolución temporal: 1 minutos\n-%% de datos transmitidos: %.2f\n-Profundidad máxima: %.2f [m]\n-Nivel de riesgo máximo: %s\n-Profundidad promedio: %.2f [m]\n*Calidad de datos a\xfan sin \n verificar exhaustivamente'%(format)
        except:
            text = u'ESTACIÓN SIN DATOS TEMPORALMENTE'

        ax4=fig.add_subplot(413)
        img=plt.imread(self.data_path+'tools/leyenda.png')
        im=ax4.imshow(img)
        pos=im.axes.get_position()
        im.axes.set_position((pos.x0-.026,pos.y0-.17,pos.x1-.026,pos.y1-.17))
        ax4.axis('off')

        ax3 = fig.add_subplot(414)
        ax3.text(0.0,1.1,'RESUMEN',color = self.colores_siata[-1])
        ax3.text(0.0, 0.0,text,linespacing=2.1,fontsize=15)
        plt.axis('off')
        plt.suptitle('%s | %s'%(self.codigo,self.info.nombre),y=0.93)
        plt.savefig(filepath,bbox_inches='tight')
        return ax,ax2,ax3

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
        self.table = 'myusers_hydrodata'
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
            obj.table = 'myusers_hydrodata'
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
        df = self.read_sql("select fecha,nivel,codigo from myusers_hydrodata where fecha between '%s' and '%s'"%(start.strftime('%Y-%m-%d %H:%M'),end.strftime('%Y-%m-%d %H:%M')))
        now = datetime.datetime.now()
        s = pd.DataFrame(df.loc[df.nivel.notnull()].groupby('codigo')['fecha'].max().sort_values())
        s['nombre'] = self.infost.loc[s.index,'nombre']
        s['delta'] = now-s['fecha']
        for horas,valor in zip([1,3,24,72],['green','yellow','orange','red']):
            r = s['fecha']<(now-datetime.timedelta(hours=horas))
            s.loc[s[r].index,'rango']=valor
        return s.dropna()

    def reporte_calidad(self,path):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        from reportlab.lib.styles import ParagraphStyle
        from reportlab.platypus import SimpleDocTemplate,Paragraph, Table, TableStyle
        from reportlab.lib.enums import TA_LEFT, TA_CENTER
        df = self.calidad()
        df = df[['nombre','fecha','delta','rango']]
        df = df.reset_index()
        s = df['rango']
        df = df.drop('rango',axis=1)
        lista = []
        for id in df.index:
            lista.append(list(df.loc[id]))
        doc = SimpleDocTemplate(path, pagesize=(520,600))
        # container for the 'Flowable' objects
        elements = []
        style = ParagraphStyle(
            name='Normal',
            aligment = TA_LEFT)

        elements.append(Paragraph("Reporte de estaciones caidas", style))
        lista.insert(0,['Código','Nombre','Último dato','Delta'])
        data = lista
        #data = df.applyvalues
        t=Table(data)
        for pos,color in zip(s.index,s.values):
            t.setStyle(TableStyle([('BACKGROUND',(0,pos+1),(0,pos+1),color)]))
        elements.append(t)

        t.setStyle(TableStyle([('BACKGROUND',(0,0),(-1,0),self.colores_siata[-1]),
                               ('TEXTCOLOR',(0,0),(-1,0),'white'),
                               ('ALIGN',(0,0),(-1,0),'CENTER')]))
        # write the document to disk
        doc.build(elements)
        #doc.drawString(200,5000,u'Nivel sin riesgo')

    def add_area_metropol(self,m,**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        path = kwargs.get('path',self.data_path+'shapes/AreaMetropolitana')
        m.readshapefile(path,'area',linewidth=0.5,color='w')
        x,y = m(self.info.longitud,self.info.latitud)
        scatterSize=100
        m.scatter(x,y,color='grey',s=120+scatterSize+60,edgecolors='grey',zorder=39)
        m.scatter(x,y,color='w',s=120+scatterSize+60,edgecolors='k',zorder=40)
        m.scatter(x,y,marker='v',color='k',s=20+scatterSize,zorder=41)
        municipios = [m.area_info[i]['Name'] for i in range(10)]
        patches=[]
        for info,shape in zip(m.area_info,m.area):
            patches.append(Polygon(np.array(shape),True),)
        plt.gca().add_collection(PatchCollection(patches,color='grey',edgecolor='w',zorder=1,alpha=0.3,label='asdf'))
        for frame in ['top','bottom','right','left']:
            plt.gca().spines[frame].set_color('w')

    def plot_rain_future(self,current_vect,future_vect,filepath=None):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        plt.close('all')
        fig = plt.figure(figsize=(16,8))
        fig.subplots_adjust(left=None,
                            bottom=None,
                            right=None,
                            top=None,
                            wspace=0.1,
                            hspace=None)
        ax1 = fig.add_subplot(121)
        m = self.plot_basin_rain(current_vect,ax=ax1)
        self.add_area_metropol(m)
        ax2 = fig.add_subplot(122)
        m = self.plot_basin_rain(future_vect,ax=ax2,cbar=True)
        self.add_area_metropol(m)
        if filepath:
            plt.savefig(filepath,bbox_inches='tight')

    def plot_level_report(self,level,rain,riesgos,fontsize=14,ncol=4,ax=None,bbox_to_anchor=(1.0,1.2),**kwargs):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        if ax is None:
            fig = plt.figure(figsize=(13.,4))
            ax = fig.add_subplot(111)
        #nivel = level.resample('H',how='mean')
        #nivel.plot(ax=ax,label='',color='k')
        mean_rain = rain
        ax.set_xlim(level.index[0],mean_rain.index[-1])
        level.plot(label='',color='k',fontsize=fontsize,lw=2,zorder=20,**kwargs)
        axu= ax.twinx()
        axu.set_ylabel('Intensidad promedio [mm/h]',fontsize=fontsize)
        mean_rain.plot(ax=axu,alpha=0.5,fontsize=fontsize,**kwargs)
        axu.fill_between(mean_rain.index,0,mean_rain.values,alpha=0.2)
        ylim = axu.get_ylim()[::-1]
        ylim = (ylim[0],0.0)
        axu.set_ylim(ylim)
        ax.set_ylabel('Profundidad (cm)',fontsize=fontsize)
        ax.set_ylim(0,riesgos[-1]*1.2)
        alpha=0.9
        ax.fill_between(level.index[:3],ax.get_ylim()[0],riesgos[0],alpha=alpha,color='g')
        ax.fill_between(level.index[:3],riesgos[0],riesgos[1],alpha=alpha,color='yellow')
        ax.fill_between(level.index[:3],riesgos[1],riesgos[2],alpha=alpha,color='orange')
        #ax.fill_between(nivel.index,riesgos[2],riesgos[3],alpha=alpha,color='red')
        ax.fill_between(level.index[:3],riesgos[2],ax.get_ylim()[1],alpha=alpha,color='red')
        scatterSize=100
        ax.hlines(level.loc[level.index[-1]],level.index[0],level.index[-1],linewidth=1.0,linestyles='dashed')
        ax.scatter(level.index[-1],level.loc[level.index[-1]],color='grey',s=120+scatterSize+60,edgecolors='grey',zorder=39)
        ax.scatter(level.index[-1],level.loc[level.index[-1]],color='w',s=120+scatterSize+60,edgecolors='k',zorder=40)
        ax.scatter(level.index[-1],level.loc[level.index[-1]],marker='v',color='k',s=20+scatterSize,zorder=41)

    def rain_report(self,date):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        date = pd.to_datetime(date)
        start = date-datetime.timedelta(minutes=150)# 3 horas atras
        end = date+datetime.timedelta(minutes=30)
        #filepaths
        local_path = self.data_path
        remote_path = 'mcano@siata.gov.co:/var/www/mario/reportes_lluvia/'
        day_path = local_path + date.strftime('%Y%m%d')+'/'
        station_path = day_path+str(self.codigo)+'/'
        filepath = station_path+self.file_format(start,end)
        #make directories
        os.system('mkdir %s'%day_path)
        os.system('mkdir %s'%station_path)
        #rain
        vec = self.radar_rain(start,end,ext='.bin')
        current_vect = vec.drop(vec.loc[date:].index).sum().values/1000
        future_vect = vec.drop(vec.loc[:date].index).sum().values/1000
        # level
        mean_rain = self.radar_rain(start,end)*12.0
        series = self.level(start,date).resample('5min').mean()
        series[series>self.info.offset] = np.NaN
        series.index.name = ''
        level_cond = (series.dropna().size/series.size) < 0.05 # condición de nivel para graficar
        rain_cond = len(current_vect)==0.0
        if level_cond:
            print ('WARNING : Not enough level data')
        if rain_cond:
            print ('WARNING : Not rain in basin')
        if level_cond or rain_cond:
            pass
        else:
            #plots
            self.plot_rain_future(current_vect,future_vect,filepath = filepath+'_rain')
            self.plot_level_report(series,mean_rain,self.risk_levels)
            plt.gca().set_xlim(start,end)
            plt.savefig(filepath+'_level.png',bbox_inches='tight')
            self.rain_report_reportlab(filepath,date)
            os.system('ssh mcano@siata.gov.co "mkdir /var/www/mario/reportes_lluvia/%s"'%(date.strftime('%Y%m%d')))
            query = "rsync -r %s %s/%s/"%(filepath+'_report.pdf',remote_path+date.strftime('%Y%m%d'),self.codigo)
            os.system(query)

    def rain_report_reportlab(self,filepath,date):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.pdfgen import canvas
        avenir_book_path = self.data_path + 'tools/AvenirLTStd-Book.ttf'
        pdfmetrics.registerFont(TTFont('AvenirBook', avenir_book_path))
        current_vect_title = 'Lluvia acumulada en la cuenca en las últimas dos horas'
        # REPORLAB
        pdf = canvas.Canvas(filepath+'_report.pdf',pagesize=(900,1200))
        cx = 0
        cy = 900
        pdf.drawImage(filepath+'_rain.png',60,650,width=830,height=278)
        pdf.drawImage(filepath+'_level.png',20,270+20,width=860,height=280)
        pdf.drawImage(self.data_path + 'tools/pie.png',0,0,width=905,height=145.451)
        pdf.drawImage(self.data_path + 'tools/cabeza.png',0,1020,width=905,height=180)
        pdf.setFillColor('#%02x%02x%02x' % (8,31,45))
        pdf.setFont("AvenirBook", 23)
        pdf.drawString(240,1045,u'Estación %s - %s'%(self.info.nombre,date.strftime('%d %B de %Y')))
        # últimas dos horas
        pdf.setFillColor('#%02x%02x%02x' % (8,31,45))
        pdf.setFont("AvenirBook", 20)
        pdf.drawString(90,520+300+30+100+30,'Lluvia acumulada en la cuenca')
        pdf.drawString(90,520+300+10+100+20,'en las últimas dos horas')
        # Futuro
        distance = 430
        pdf.drawString(90+distance-10,520+300+100+40+20,'Lluvia acumulada en la cuenca')
        pdf.drawString(90+distance-10,520+300+10+100+20,'en la próxima media hora')
        pdf.drawString(90,270+280+10+20,'Profundidad de la lámina de agua e intensidad promedio en la cuenca')
        #pdf.setFont("AvenirBook", 15)
        pdf.drawImage(self.data_path + 'tools/leyenda.png',67,180,width=800,height=80)
        #N1
        pdf.setFillColor('#%02x%02x%02x' % (8,31,45))
        pdf.setFont("AvenirBook", 15)
        pdf.drawString(80,210,'Nivel seguro')
        #pdf.drawString(90+distance,520+300+10+100,'en la próxima media hora')
        factor = 143
        pdf.drawString(80+factor,210,'Nivel de ')
        pdf.drawString(80+factor,190,'alerta')
        #N2
        factor = 287
        pdf.drawString(80+factor,210,'Inundación')
        pdf.drawString(80+factor,190,'menor')
        #N4
        factor = 430
        pdf.drawString(80+factor,210,'Inundación')
        pdf.drawString(80+factor,190,'mayor')
        #N4
        factor = 430
        y = 30
        pdf.drawString(700,210+y,'Intensidad de lluvia')
        pdf.drawString(700,180+y,'Profundidad')
        pdf.drawString(700,160+y-5,'Profundidad actual')
        pdf.showPage()
        pdf.save()

    def make_rain_report_current(self,codigos):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        for codigo in codigos:
            nivel = cpr.Nivel(codigo = codigo,SimuBasin=True,**info.LOCAL)
            nivel.rain_report(datetime.datetime.now())

    def risk_df(self,df):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        df = df.copy()
        for codigo in df.columns:
            risk_levels = np.array(self.infost.loc[codigo,['n1','n2','n3','n4']])
            try:
                df[codigo] = df[codigo].apply(lambda x:self.convert_level_to_risk(x,risk_levels))
            except:
                df[codigo] = np.NaN
        df = df[df.sum().sort_values(ascending=False).index].T
        return df

    def make_risk_report(self,df,figsize=(6,14),bbox_to_anchor = (-0.15, 1.09),ruteSave = None,legend=True):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        def make_colormap(seq):
            seq = [(None,) * 3, 0.0] + list(seq) + [1.0, (None,) * 3]
            cdict = {'red': [], 'green': [], 'blue': []}
            for i, item in enumerate(seq):
                if isinstance(item, float):
                    r1, g1, b1 = seq[i - 1]
                    r2, g2, b2 = seq[i + 1]
                    cdict['red'].append([item, r1, r2])
                    cdict['green'].append([item, g1, g2])
                    cdict['blue'].append([item, b1, b2])
            return mcolors.LinearSegmentedColormap('CustomMap', cdict)
        df = df.loc[df.index[::-1]]
        c = mcolors.ColorConverter().to_rgb
        cm = make_colormap([c('green'),0.20,c('#f2e71d'),0.4,c('orange'),0.60,c('red'),0.80,c('red')])
        fig = plt.figure(figsize=figsize)
        im = plt.imshow(df.values, interpolation='nearest', vmin=0, vmax=4, aspect='equal',cmap=cm);
        #cbar = fig.colorbar(im)
        ax = plt.gca();
        ax.set_xticks(np.arange(0,df.columns.size, 1));
        ax.set_yticks(np.arange(0, df.index.size, 1));
        ax.set_xticklabels(df.columns,fontsize=14);
        ax.set_yticklabels(df.index,fontsize=14,ha = 'left');
        ax.set_xticks(np.arange(-.5, df.columns.size, 1), minor=True,);
        ax.set_yticks(np.arange(-.5, df.index.size, 1), minor=True);
        plt.draw()
        yax = ax.get_yaxis()
        pad = max(T.label.get_window_extent().width*1.05 for T in yax.majorTicks)
        yax.set_tick_params(pad=pad)
        ax.invert_yaxis()
        ax.xaxis.tick_top()
        #ax.Axes.tick_params(axis='x', rotation=45)
        ax.grid(which='minor', color='w', linestyle='-', linewidth=2)
        plt.xticks(rotation=90)
        alpha=1
        height = 8

    def level_all(self,start=None,end = None,codigos=None,hours=3,local=False,**kwargs):
        '''
        Gets last topo-batimetry in db
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
        codigos = kwargs.get('codigos',self.infost.index)
        if local:
            fields = 'estaciones_estaciones.codigo,myusers_hydrodata.fecha,myusers_hydrodata.profundidad'
            join = 'estaciones_estaciones ON myusers_hydrodata.fk_id = estaciones_estaciones.id'
            format = (fields,join,start.strftime(self.date_format),end.strftime(self.date_format))
            query = "SELECT %s FROM myusers_hydrodata INNER JOIN %s WHERE myusers_hydrodata.fecha between '%s' and '%s' "%format
            df = self.read_sql(query).set_index(['codigo','fecha']).unstack(0)['profundidad']
            return df[codigos]
        else:
            start = self.round_time(pd.to_datetime(start))
            df = pd.DataFrame(index = pd.date_range(start,end,freq='1min'),columns = codigos)
            for codigo in codigos:
                try:
                    level = Nivel(codigo=codigo,** info.LOCAL).level(start,end,**kwargs)
                    df[codigo] = level
                except:
                    pass
            return df

    def make_risk_report_current(self,df):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        # estaciones en riesgo
        df = df.copy()
        in_risk = df.T
        in_risk = in_risk.sum()[in_risk.sum()!=0.0].index.values
        df.columns = map(lambda x:x.strftime('%H:%M'),df.columns)
        df.index = np.array(df.index.values,str)+(np.array([' | ']*df.index.size)+self.infost.loc[df.index,'nombre'].values)
        self.make_risk_report(df,figsize=(15,25))
        filepath = self.data_path + 'reportes/reporte_niveles_riesgo_actuales.png'
        plt.savefig(filepath,bbox_inches='tight')
        os.system('scp %s mcano@siata.gov.co:/var/www/mario/realTime/'%filepath)
        return in_risk

    def reporte_nivel(self):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        def convert_to_risk(df):
            df = self.risk_df(df)
            return df[df.columns.dropna()]
        self.make_risk_report_current(convert_to_risk(self.level_all().resample('5min').max()))

    def rain_area_metropol(self,vec,ax,f=1):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        cmap_radar,levels,norm = self.radar_cmap()
        extra_lat,extra_long = self.adjust_basin(fac=0.02)
        extra_long=0
        extra_lat=0
        kwargs = {}
        contour_keys={'cmap'  :cmap_radar,
                    'levels':levels,
                    'norm'  :norm}
        perimeter_keys={'color':'k','linewidth':1}
        longs,lats=self.longitude_latitude_basin()
        x,y=np.meshgrid(longs,lats)
        y=y[::-1]
        # map settings
        m = Basemap(projection='merc',llcrnrlat=lats.min()-0.05*f, urcrnrlat=lats.max()+0.05*f,
        llcrnrlon=longs.min()-0.05*f, urcrnrlon=longs.max()+0.1*f, resolution='c',ax=ax,**kwargs)
        # perimeter plot
        xp,yp = m(self.Polygon[0], self.Polygon[1])
        m.plot(xp, yp,**perimeter_keys)
        # vector plot
        if vec is not None:
            map_vec,mxll,myll=wmf.cu.basin_2map(self.structure,vec,len(longs),len(lats),self.ncells)
            map_vec[map_vec==wmf.cu.nodata]=np.nan
            xm,ym=m(x,y)
            contour = m.contourf(xm, ym, map_vec.T, 25,**contour_keys)
        else:
            contour = None
        m.readshapefile(self.data_path+ 'shapes/AreaMetropolitana','area',linewidth=0.5,color='w')
        m.readshapefile(self.data_path+ 'shapes/net/%s/%s'%(self.codigo,self.codigo),str(self.codigo))
        m.readshapefile(self.data_path+ 'shapes/streams/%s/%s'%(self.codigo,self.codigo),str(self.codigo))
        x,y = m(self.info.longitud,self.info.latitud)
        #m.scatter(x,y,s=100,zorder=10)
        scatterSize=100
        m.scatter(x,y,color='grey',s=120+scatterSize+60,edgecolors='grey',zorder=39)
        m.scatter(x,y,color='w',s=120+scatterSize+60,edgecolors='k',zorder=40)
        m.scatter(x,y,marker='v',color='k',s=20+scatterSize,zorder=41)
        municipios = [m.area_info[i]['Name'] for i in range(10)]
        patches=[]
        for info,shape in zip(m.area_info,m.area):
            patches.append(Polygon(np.array(shape),True),)
        ax.add_collection(PatchCollection(patches,color='grey',edgecolor='w',zorder=1,alpha=0.3,label='asdf'))
        for frame in ['top','bottom','right','left']:
            ax.spines[frame].set_color('w')
        cbar = m.colorbar(contour,location='right',pad="5%")

    def convert_series_to_risk(self,level):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        risk = level.copy()
        colors = ['green','gold','orange','red','red','black']
        for codigo in level.index:
            try:
                risks = cpr.Nivel(codigo = codigo,**info.LOCAL).risk_levels
                risk[codigo] = colors[int(self.convert_level_to_risk(level[codigo],risks))]
            except:
                risk[codigo] = 'black'
        return risk

    @logger
    def reporte_lluvia(self,end,filepath=None):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        self = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
        #end = datetime.datetime.now()
        start = end - datetime.timedelta(hours=3)
        posterior = end + datetime.timedelta(minutes=10)
        rain = self.radar_rain(start,posterior)
        rain_vect = self.radar_rain_vect(start,posterior)
        codigos = self.infost.index
        df = pd.DataFrame(index = rain_vect.index,columns=codigos)
        for codigo in codigos:
            mask_path = self.data_path + 'mask/mask_%s.tif'%(codigo)
            try:
                mask_map = wmf.read_map_raster(mask_path)
                mask_vect = self.Transform_Map2Basin(mask_map[0],mask_map[1])
            except AttributeError:
                mask_vect = None
            if mask_vect is not None:
                mean = []
                for date in rain_vect.index:
                    try:
                        mean.append(np.sum(mask_vect*rain_vect.loc[date])/np.sum(mask_vect))
                    except:
                        pass
                if len(mean)>0:
                    df[codigo] = mean
        df_posterior = df.loc[end:]
        plt.rc('font', **{'size'   :16})
        fig = plt.figure(figsize=(20,20))
        fig.subplots_adjust(hspace=1.1)
        ax1 = fig.add_subplot(211)
        ax2 = fig.add_subplot(212)
        suma = (df/1000.).sum().sort_values(ascending=False)
        suma = suma[suma>0.0]
        orden = np.array(suma.index,int)
        suma.index = self.infost.loc[suma.index,'nombre']
        risk = self.convert_series_to_risk(self.level_all(hours=1).iloc[-3:].max())
        dfb = pd.DataFrame(index=suma.index,columns=['rain','color'])
        dfb['rain'] = suma.values
        dfb['color'] = risk.loc[orden].values
        dfb.plot.bar(y='rain', color=[dfb['color']],ax=ax1)
        #suma.plot(kind='bar',color = list(),ax=ax1)
        title = 'start: %s, end: %s'%(start.strftime('%Y-%m-%d %H:%M'),end.strftime('%Y-%m-%d %H:%M'))
        filepath = self.data_path + 'reportes/lluvia_en_cuencas.png'
        ax1.set_title(title)
        ax1.set_ylabel('lluvia acumulada\n promedio en la cuenca [mm]')
        suma = (df_posterior/1000.).sum().loc[orden]
        suma.index = self.infost.loc[suma.index,'nombre']
        dfb = pd.DataFrame(index=suma.index,columns=['rain','color'])
        dfb['rain'] = suma.values
        dfb['color'] = risk.loc[orden].values
        dfb.plot.bar(y='rain', color=[dfb['color']],ax=ax2)
        #suma.plot(kind='bar',ax=ax2)
        filepath = self.data_path + 'reportes/lluvia_en_cuencas.png'
        ax2.set_title(u'lluvia acumulada en la próxima media hora')
        ax2.set_ylabel('lluvia acumulada\n promedio en la cuenca [mm]')
        ax1.set_ylim(0,30)
        ax2.set_ylim(0,30)
        if filepath:
            plt.savefig(filepath,bbox_inches='tight')

    def plot_risk_daily(self,df,bbox_to_anchor = (-0.15, 1.09),figsize=(6,14),ruteSave = None,legend=True,fontsize=20):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        def make_colormap(seq):
            seq = [(None,) * 3, 0.0] + list(seq) + [1.0, (None,) * 3]
            cdict = {'red': [], 'green': [], 'blue': []}
            for i, item in enumerate(seq):
                if isinstance(item, float):
                    r1, g1, b1 = seq[i - 1]
                    r2, g2, b2 = seq[i + 1]
                    cdict['red'].append([item, r1, r2])
                    cdict['green'].append([item, g1, g2])
                    cdict['blue'].append([item, b1, b2])
            return mcolors.LinearSegmentedColormap('CustomMap', cdict)
        df = df.loc[df.index[::-1]]
        c = mcolors.ColorConverter().to_rgb
        cm = make_colormap([c('green'),0.20,c('#f2e71d'),0.4,c('orange'),0.60,c('red'),0.80,c('red')])
        fig = plt.figure(figsize=figsize)
        im = plt.imshow(df.values, interpolation='nearest', vmin=0, vmax=4, aspect='equal',cmap=cm);
        #cbar = fig.colorbar(im)
        ax = plt.gca();
        ax.set_xticks(np.arange(0,df.columns.size, 1));
        ax.set_yticks(np.arange(0, df.index.size, 1));
        ax.set_xticklabels(df.columns,fontsize=fontsize);
        ax.set_yticklabels(df.index,fontsize=fontsize,ha = 'left');
        ax.set_xticks(np.arange(-.5, df.columns.size, 1), minor=True,);
        ax.set_yticks(np.arange(-.5, df.index.size, 1), minor=True);
        plt.draw()
        yax = ax.get_yaxis()
        pad = max(T.label.get_window_extent().width*1.05 for T in yax.majorTicks)
        yax.set_tick_params(pad=pad)
        ax.invert_yaxis()
        ax.xaxis.tick_top()
        ax.grid(which='minor', color='w', linestyle='-', linewidth=2)
        #ax.text(-0.4,df.index.size+0.5,'NIVELES DE RIESGO\n %s - %s'%(start,pd.to_datetime(end).strftime('%Y-%m-%d')),fontsize=16)
        alpha=1
        height = 8
        for tick in ax.get_xticklabels():
            tick.set_rotation(90)

    @logger
    def reporte_diario(self,date):
        '''
        Gets last topo-batimetry in db
        Parameters
        ----------
        x_sensor   :   x location of sensor or point to adjust topo-batimetry
        Returns
        ----------
        last topo-batimetry in db, DataFrame
        '''
        end = pd.to_datetime(pd.to_datetime(date).strftime('%Y-%m-%d')+' 23:55') - datetime.timedelta(days=1)
        start = (end-datetime.timedelta(days=6)).strftime('%Y-%m-%d 00:00')
        folder_path = self.data_path + 'reporte_diario/%s'%end.strftime('%Y%m%d')
        os.system('mkdir %s'%folder_path)
        df = self.level_all(start,end)
        try:
            df = df.T.drop([1013,1014,195,196]).T
        except:
            pass
        daily = df.resample('D').max()
        rdf = self.risk_df(daily)
        # niveles de riesgo en el último día
        last_day_risk = rdf[rdf.columns[-1]].copy()
        last_day_risk = last_day_risk[last_day_risk>0.0].sort_values(ascending=False).index
        rdf = rdf.loc[rdf.max(axis=1).sort_values(ascending=False).index]
        rdf = rdf[rdf.max(axis=1)>0.0]
        rdf = rdf.fillna(0)
        labels = []
        for codigo,nombre in zip(self.infost.loc[rdf.index].index,self.infost.loc[rdf.index,'nombre'].values):
            labels.append('%s | %s'%(codigo,nombre))
        rdf.index = labels
        def to_col_format(date):
            return (['L','M','MI','J','V','S','D'][int(date.strftime('%u'))-1]+date.strftime('%d'))
        rdf.columns = map(lambda x:to_col_format(x),rdf.columns)
        import sys
        # sys.setdefaultencoding() does not exist, here!
        reload(sys)  # Reload does the trick!
        sys.setdefaultencoding('UTF8')
        self.plot_risk_daily(rdf,figsize=(14,20))
        plt.savefig(folder_path+'/reporte_nivel.png',bbox_inches='tight')
        remote_path = 'mcano@siata.gov.co:/var/www/mario/reporte_diario/'
        query = "rsync -r %s %s/"%(folder_path+'/reporte_nivel.png',remote_path+end.strftime('%Y%m%d'))
        os.system(query)
        #Graficas
        fontsize = 25
        font = {'size'   :fontsize}
        plt.rc('font', **font)
        filepath = None

        for num,codigo in enumerate(np.array(last_day_risk,int)):
            obj = Nivel(codigo=codigo,SimuBasin=False,**info.LOCAL)
            series = df.loc[daily.index[-1].strftime('%Y-%m-%d'),codigo]
            series = obj.level(series.index[0],series.index[-1])
            if series.dropna().index.size==0.0:
                pass
            else:
                plt.figure()
                f, (ax1, ax2) = plt.subplots(1, 2, sharey=True,figsize=(25,4))
                obj.plot_level(series/100.0,
                                lamina='max',
                                risk_levels=np.array(obj.risk_levels)/100.0,
                                legend=False,
                                resolution='m',
                                ax=ax1,
                                scatter_size=40)

                alpha=0.2
                bat = obj.last_bat(obj.info.x_sensor)
                ymax = max([bat['y'].max(),(obj.risk_levels[-1])/100.0])
                lamina = 'max'
                    # plot section
                if series.dropna().index.size == 0:
                    lamina = 0.0
                else:
                    if lamina == 'current':
                        x,lamina = (series.dropna().index[-1],series.dropna().iloc[-1])
                    else:
                        x,lamina = (series.argmax(),series.max())

                sections =obj.plot_section(bat,
                                        ax = ax2,
                                        level=lamina/100.0,
                                        riskLevels=np.array(obj.risk_levels)/100.0,
                                        xSensor=obj.info.x_sensor,
                                        scatterSize=50)
                major_locator        = mdates.DayLocator(interval=5)
                formater = '%H:%M'
                ax1.xaxis.set_major_formatter(mdates.DateFormatter(formater))
                ax1.set_xlabel(u'Fecha')
                ax2.spines['top'].set_color('w')
                ax2.spines['right'].set_color('w')
                ax2.spines['right'].set_color('w')
                ax2.set_ylim(bat['y'].min(),ymax)
                ax1.set_ylim(bat['y'].min(),ymax)
                ax1.set_title(u'código: %s'%codigo)
                ax2.set_title('Profundidad en el canal')
                ax2.set_ylabel('Profundidad [m]')
                #ax1.set_xlabel('03 Mayo - 04 Mayo')
                ax1.annotate(u'máximo', (mdates.date2num(series.argmax()), series.max()/100.0), xytext=(10, 10),textcoords='offset points',fontsize=fontsize)
                #file = 'section_%s.png'%(num+1)
                ax2.set_title(obj.info.nombre)
                #filepath = 'reportes_amva/%s.png'%codigo
                for tick in ax1.get_xticklabels():
                    tick.set_rotation(45)
                filepath = folder_path+'/'+obj.info.slug+'.png'
                plt.savefig(filepath,bbox_inches='tight')
                os.system('rsync %s %s'%(filepath,remote_path+end.strftime('%Y%m%d')+'/'))

        obj = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
        radar_rain = obj.radar_rain_vect(start,end)
        diario = radar_rain.resample('D').sum()
        rain = obj.radar_rain(start,end)
        fig = plt.figure(figsize=(20,20))
        for pos,dia in enumerate(diario.index):
            ax = fig.add_subplot(3,3,pos+1)
            obj.rain_area_metropol(diario.loc[dia].values/1000.0,ax)
            ax.set
            plt.gca().set_title(rdf.columns[pos])
        plt.savefig(folder_path+'/lluvia_diaria.png',bbox_inches='tight')
        remote_path = 'mcano@siata.gov.co:/var/www/mario/reporte_diario/'
        query = "rsync -r %s %s/"%(folder_path+'/lluvia_diaria.png',remote_path+end.strftime('%Y%m%d'))
        os.system(query)

    @logger
    def gif(self,start,end,delay=0,loop=0):
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
        rain_vect = self.radar_rain_vect(start,end)
        rain = self.radar_rain(start,end)*12.0
        bat = self.last_bat(self.info.x_sensor)
        nivel = self.level(start,end).resample('5min',how='mean')
        rain_vect = rain_vect.reindex(nivel.index)
        rain = rain.reindex(nivel.index)
        filepath = self.data_path + 'user_output/gifs/%s/'%self.file_format(start,end)
        os.system('mkdir %s'%filepath)
        def plot_gif(count,fecha,f=1,filepath=filepath,**kwargs):
            fontsize = 18
            font = {'size'   :fontsize}
            plt.rc('font', **font)
            series = nivel.copy()
            series[count:] = np.NaN
            vect = rain_vect.copy()
            vect = vect.drop(vect.index[count:])
            s = rain.copy()
            s[count:] = np.NaN
            level = series.dropna().iloc[-1]/100.0
            vec = vect.sum().values/1000.0
            scatterSize = 70
            figsize=(18,14)
            series = pd.Series.copy(series/100.0)
            risk_levels = np.array(self.risk_levels,float)/100.0
            fig = plt.figure(figsize=figsize)
            fig.subplots_adjust(wspace=0.3)
            #gs = GridSpec(3, 3)
            ax1 = fig.add_subplot(2,2,1)
            # identical to ax1 = plt.subplot(gs.new_subplotspec((0,0), colspan=3))
            ax2 = fig.add_subplot(2,2,2,sharey=ax1)
            ylimit = kwargs.get('ylimit',max(risk_levels)*1.05)
            series.plot(ax=ax1,label='',color='w',linewidth=0.001,fontsize=fontsize,**kwargs)
            #ax1.fill_between(series.index,series.values,color=self.colores_siata[0])
            ax1.fill_between(series.index, series.values,color=self.colores_siata[1],label='Open values')
            #ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M\n%d %b'))
            alpha=0.2
            ymax = max([bat['y'].max(),risk_levels[-1]])
            ax1.set_xlim(series.index[0],series.index[-1]+datetime.timedelta(minutes=5))
            sections =self.plot_section(bat,
                                   ax = ax2,
                                   level=level,
                                   riskLevels=risk_levels,
                                   xSensor=self.info.x_sensor,
                                   scatterSize=scatterSize)
            ax1.spines['top'].set_color('w')
            ax1.spines['right'].set_color('w')
            ax2.spines['top'].set_color('w')
            ax2.spines['right'].set_color('w')
            ax2.spines['right'].set_color('w')
            ax1.set_ylabel('Profundidad [m]')
            ax1.set_xlim(rain.index[0],rain.index[-1])
            ax1.scatter(series.dropna().index[-1],level,marker='v',color='k',s=30+scatterSize,zorder=22)
            ax1.scatter(series.dropna().index[-1],level,color='white',s=120+scatterSize+10,edgecolors='k')
            ax3 = fig.add_subplot(2,2,3,sharex=ax1)
            s.plot(ax=ax3,color='w',linewidth=0.001,fontsize=fontsize)
            ax3.fill_between(s.index, s.values,color=self.colores_siata[3],label='O')
            ax3.spines['top'].set_color('w')
            ax3.spines['right'].set_color('w')
            ax3.spines['right'].set_color('w')
            ax4 = fig.add_subplot(2,2,4)
            self.rain_area_metropol(vec,ax4,f=f)
            ax2.set_ylim(0,ymax)
            #cb = plt.colorbar(ax, cax = cbaxes)
            #cb.set_level('[mm]')
            ax1.set_title(u'Profundidad de la lámina de agua')
            ax2.set_title(u'Sección transversal del canal')
            ax3.set_title(u'Lluvia promedio en la cuenca')
            ax3.set_ylabel('Intensidad promedio [mm/h]',fontsize=fontsize)
            ax4.set_title(u'Lluvia acumulada [mm]')
            ax3.set_ylim(0,rain.max())
            ax3.set_xlim(rain.index[0],rain.index[-1])
            plt.suptitle(u'%s | %s'%(self.info.nombre,fecha))
            path = '%s%.3d.png'%(filepath,count)
            plt.savefig(path,bbox_inches='tight')
        # loop
        for count in range(1,rain.index.size+1):
                plot_gif(count,self.info.nombre,f=0.5)

        file_name = self.file_format(start,end)+'-gif'
        query = "convert -delay %s -loop %s %s*.png %s%s.gif"%(delay,loop,filepath,filepath,file_name)
        r = os.system(query)
        r=0
        if r ==0:
            print('INFO: gif saved in path: %s%s'%(filepath,file_name))
        else:

            print ('didnt work')
        filepath = filepath+file_name
        remote_path = 'mcano@siata.gov.co:/var/www/mario/gifs/'
        os.system('ssh mcano@siata.gov.co "mkdir /var/www/mario/gifs/%s"'%(end.strftime('%Y%m%d')))
        query = "rsync -r %s.gif %s/%s/"%(filepath,remote_path+end.strftime('%Y%m%d'),self.codigo)
        return os.system(query)

    def fecha_hora_format_data(self,field,start,end,**kwargs):
        '''
        Gets pandas Series with data from tables with
        date format fecha and hora detached, and filter
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
        start= pd.to_datetime(start).strftime('%Y-%m-%d %H:%M:00')
        end = pd.to_datetime(end).strftime('%Y-%m-%d %H:%M:00')
        format = (field,self.codigo,self.fecha_hora_query(start,end))
        sql = SqlDb(codigo = self.codigo,**info.REMOTE)
        if kwargs.get('calidad'):
            df = sql.read_sql("SELECT fecha,hora,%s from datos WHERE calidad = '1' and cliente = '%s' and %s"%format)
        else:
            df = sql.read_sql("SELECT fecha,hora,%s from datos WHERE cliente = '%s' and %s"%format)
        # converts centiseconds in 0
        try:
            df['hora'] = df['hora'].apply(lambda x:x[:-3]+':00')
        except TypeError:
            df['hora']=df['hora'].apply(lambda x:str(x)[-8:-8+5]+':00')
            df['fecha'] = df['fecha'].apply(lambda x:x.strftime('%Y-%m-%d'))
        # concatenate fecha and hora fields, and makes nan bad datetime indexes
        df.index= pd.to_datetime(df['fecha'] + ' '+ df['hora'],errors='coerce')
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

    @logger
    def insert_myusers_hydrodata(self,start=None,end=None,df=None):
        '''
        Inserts data into myusers_hydrodata table, if fecha and fk_id exist, updates values.
        bad data
        Parameters
        ----------
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas time Series
        '''
        if df is None:
            df = self.level_all(start,end,calidad=True)
        else:
            df = df.copy()
        query = "INSERT INTO myusers_hydrodata (fk_id,fecha,profundidad,timestamp,updated,user_id) VALUES "
        df = df.unstack().reset_index()
        df.columns = ['fk_id','fecha','profundidad']
        df['profundidad'] = df['profundidad']
        df['fk_id'] = self.infost.loc[np.array(df['fk_id'].values,int),'id'].values
        df['profundidad'] = df['profundidad'].apply(lambda x:round(x,3))
        df = df.applymap(lambda x:str(x))
        df['timestap'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        df['updated'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M')
        df['user_id'] = '1'
        for id,s in df.iterrows():
            query+=('('+str(list(s.values)).strip('[]'))+'), '
        query = query[:-2]
        query = query.replace("'nan'",'NULL')
        query += ' ON DUPLICATE KEY UPDATE profundidad = VALUES(profundidad)'
        self.execute_sql(query)

    def reflectividad_to_rain(self,start=None,end=None,verbose = None,a_yuter=15,b_yuter=30,metodo = 'yuter',extrapol=False,**kwargs):
        '''
        Converts radar raw data to rain intensity
        Parameters
        ----------
        kwargs   :   start,end (window to generate rain nc files),rutaRadar,rutaNC
        Returns
        ----------
        radar rain .nc
        '''
        dateText = datetime.datetime.now().strftime('%Y%m%d%H%M')
        if start is None:
            start =  datetime.datetime.now() + datetime.timedelta(hours = 5) - datetime.timedelta(minutes = 15)
            end =  datetime.datetime.now() + datetime.timedelta(hours = 5) + datetime.timedelta(minutes = 15)
        else:
            start = pd.to_datetime(start)
            end = pd.to_datetime(end)
        hora1 = start.strftime('%H:%M')
        hora2  = end.strftime('%H:%M')
        rutaRadar = kwargs.get('rutaRadar',self.data_path + 'radar/')
        rutaNC = kwargs.get('rutaNC',self.data_path + '101_RadarClass/')
        #Si tiene que revisar en las carpetas de radar lo hace
        if extrapol == False:
            datesDias = pd.date_range(start,end,freq='D')
            a = pd.Series(np.zeros(len(datesDias)),index=datesDias)
            a = a.resample('A').sum()
            Anos = [i.strftime('%Y') for i in a.index.to_pydatetime()]

            datesDias = [d.strftime('%Y%m%d') for d in datesDias.to_pydatetime()]

            ListDays = []
            ListRutas = []
            for d in datesDias:
                    try:
                        L = os.listdir(rutaRadar+d)
                        L = [l for l in L if any(map(l.startswith,Anos)) and l.endswith('010_120.bin')]
                        ListDays.extend(L)
                        Ruta = [rutaRadar+d+'/'+l for l in L if any(map(l.startswith,Anos)) and l.endswith('010_120.bin')]
                        ListRutas.extend(Ruta)
                    except:
                        pass
            ListDays.sort()
            ListRutas.sort()
            ListRutasFin = []
            # Obtiene la ruta final teniendo en cuenta la ruta del dia en las carpetas de radar
            for j,i in zip(ListRutas, ListDays):
                d = datetime.datetime.strptime(i[:-12],'%Y%m%d%H%M')
                if d >= start and d<= end:
                    ListRutasFin.append(j)
        #Si es extrapolacion no tiene que revisar en las carpetas de radar
        else:
            ListRutasFin = os.listdir(rutaRadar)
            ListRutasFin.sort()
            ListRutasFin = [rutaRadar + i for i in ListRutasFin]
        #CONVERSION, CLASIFICACION Y GUARDADO
        #Carga la cuenca del AMVA
        Niter = len(ListRutasFin)
        Conv = np.zeros(Niter)
        Stra = np.zeros(Niter)
        Total = np.zeros(Niter)
        rad = radar.radar_process()
        for c,l in enumerate(ListRutasFin):
            Total[c] = 0
            Conv[c] = 0
            Stra[c] = 0
            rad.read_bin(l)
            rad.Class2ConvStratiform(metodo=metodo, b_yuter = b_yuter, a_yuter = a_yuter)
            rad.DBZ2Rain()
            #Obtiene la ref media
            Conv[c] = rad.Z[rad.ConvStra == 2].sum() / 1e6
            Stra[c] = rad.Z[rad.ConvStra == 1].sum() / 1e6
            #guarda
            if extrapol == False:
                rad.save_rain_class(rutaNC+l[-24:-3]+'nc')
            else:
                rad.save_rain_class(rutaNC+l[-24:-4]+'_extrapol.nc')
            Total[c] = Conv[c] + Stra[c]
