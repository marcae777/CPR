#!/usr/bin//env python
# -*- coding: utf-8 -*-
#
#
#  Copyright 2018 Sebastián Ospina <seospina@gmail.com>
from cpr.Nivel import Nivel
from cpr.SqlDb import SqlDb
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import cpr.information as info
import matplotlib.dates as mdates

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

class Calidad_Agua(Nivel):
    '''
    Provide functions to manipulate data related
    to a water quality sensor and its basin.
    '''
    local_table  = 'estaciones_estaciones'
    remote_table = 'estaciones'

    def __init__(self, codigo_calidad = 297, codigo_nivel = 240, SimuBasin = False, remote = True,**kwargs):
        '''
        The instance inherits modules to manipulate SQL
        data and uses (watershed modeling framework) wmf
        Parameters
        ----------
        codigo        : primary key
        remote_server :
        local_server  : database kwargs to pass into the Sqldb class
        nc_path       : path of the .nc file to set wmf class
        '''
        import matplotlib
        import os
        import matplotlib.font_manager as fm

        if remote:
            kwargs = info.REMOTE_CALIDAD_AGUA
            info.REMOTE = info.REMOTE_CALIDAD_AGUA
        else:
            kwargs = info.LOCAL

        self.codigo_calidad = codigo_calidad
        self.data_path = info.DATA_PATH_CALIDAD_AGUA
        self.rain_path = info.DATA_PATH_CALIDAD_AGUA + 'user_output/radar/'
        self.radar_path = info.RADAR_PATH

        SqlDb.__init__(self,codigo=codigo_calidad,**kwargs)
        self.nivel = Nivel(codigo = codigo_nivel,**kwargs)

        if SimuBasin:
            try:
                nc_path = self.infost.loc[self.codigo_calidad,'nc_path']
            except:
                print('No basin found for the given path')
            wmf.SimuBasin.__init__(self,rute=nc_path)

        self.colores_siata = [(82 /255., 183/255.,196/255.),(55 /255., 123/255.,148/255.),\
                             (43 /255.,  72/255.,105/255.),(32 /255.,  34/255., 72/255.),\
                             (34 /255.,  71/255., 94/255.),(31 /255., 115/255.,116/255.),\
                             (39 /255., 165/255.,132/255.),(139/255., 187/255.,116/255.),\
                             (200/255., 209/255., 93/255.),(249/255., 230/255., 57/255.)]

        font_path=info.DATA_PATH+'tools/AvenirLTStd-Book.otf'

        if os.path.isfile(font_path):
            self.fontype=fm.FontProperties(fname=font_path)
            self.fontype.set_size(15)
            self.legendfont=self.fontype
            self.legendfont.set_size(12)
        else:
            self.fontype=fm.FontProperties(fname=font_path)
            fonts=[i for i in matplotlib.font_manager.findSystemFonts() if ('free' in i.lower())&('sans' in i.lower())]
            self.fontype=fm.FontProperties(fname=fonts[0])

    def __repr__(self):
        return "{}. {}".format(self.codigo_calidad,self.info.Nombreestacion)

    @property
    def infost(self):
        '''
        Gets full information from all water quality stations
        Returns
        ---------
        pd.DataFrame
        '''
        self.path_info_estaciones = info.DATA_PATH_CALIDAD_AGUA + '/estaciones/info_calidad_agua.msg'
        return pd.read_msgpack(self.path_info_estaciones, encoding = 'utf-8')

    @property
    def info(self):
        '''
        Returns basic information of the water quality station corresponding to the code used to create the instansce
        '''
        return self.infost.loc[self.codigo_calidad]

    def get_slug(self,string=None):
        slug=lambda x:x.decode('utf8').encode('ascii', errors='ignore').replace('.','').replace(' ','').replace('(','').replace(')','')
        string=str(self.self.codigo_calidad)+'-'+self.info['Nombreestacion'] if string==None else string
        return slug(string)

    @property
    def slug(self):
        return self.get_slug()

    def modify_station_data(self,codigo,area=None,ce=None,do=None,orp=None,ph=None,t=None,nc_path=None,net_path=None,stream_path=None,polygon_path=None,slug=None):
        '''
        Allows to modify the basic information of a single water quality station, rewriting the msgpack file conatining the stations info.
        '''
        props={'area':area,'ce':ce,'do':do,'orp':orp,'ph':ph,'t':t,'nc_path':nc_path,'net_path':net_path,'stream_path':stream_path,'polygon_path':polygon_path,'slug':slug}

        infost=self.infost

        for prop in props.items():
            if prop[1]!=None:
                print(prop)
                infost.loc[self.codigo_calidad,prop[0]]=prop[1]

        infost.to_msgpack(self.path_info_estaciones)
        return self.info

    def parse_time(self,time_str):
        import re
        regex = re.compile(r'((?P<weeks>\d+?)w)?((?P<days>\d+?)d)?((?P<hours>\d+?)h)?')
        parts = regex.match(time_str)
        if not parts:
            return
        parts = parts.groupdict()
        time_params = {}
        for (name, param) in parts.iteritems():
            if param:
                time_params[name] = int(param)
        return datetime.timedelta(**time_params)

    def autolabel(self,rects, ax,rot,fs):
        # Get y-axis height to calculate label position from.
        (y_bottom, y_top) = ax.get_ylim()
        y_height = y_top - y_bottom

        for rect in rects:
            height = rect.get_height()

            # Fraction of axis height taken up by this rectangle
            p_height = (height / y_height)

            # If we can fit the label above the column, do that;
            # otherwise, put it inside the column.
            if p_height > 0.95: # arbitrary; 95% looked good to me.
                label_position = height - (y_height * 0.05)
            else:
                label_position = height + (y_height * 0.01)

            if np.isfinite(height):
                ax.text(rect.get_x() + rect.get_width()/2., label_position,'%.1f' % float(height),fontproperties=self.fontype,ha='center', va='bottom',rotation=rot,fontsize=fs)

    def sensor_data(self,start,end):
        '''
        Reads remote sensor water quality data
        Parameters
        ----------
        variable     : water quality varibale desired to get from siata Data Base
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas DataFrame
        '''
        sql = SqlDb(codigo = self.codigo_calidad,**info.REMOTE_CALIDAD_AGUA)
        start=pd.to_datetime(start).strftime('%Y-%m-%d %H:%M:00')
        end=pd.to_datetime(end).strftime('%Y-%m-%d %H:%M:00')
        s = sql.read_sql('select fecha_hora,ce,do,ph,orp,t,calidad from calidad_agua where fecha_hora between "%s" and "%s" order by fecha_hora'%(start,end)).set_index('fecha_hora')
        return s

    def sensor_variable(self,variable,start,end):
        '''
        Reads remote sensor water quality variable data
        Parameters
        ----------
        variable     : water quality varibale desired to get from siata Data Base (CE,DO,ORP,pH,t)
        start        : initial date
        end          : final date
        Returns
        ----------
        pandas time series
        '''
        sql = SqlDb(codigo = self.codigo_calidad,**info.REMOTE_CALIDAD_AGUA)
        start=pd.to_datetime(start).strftime('%Y-%m-%d %H:%M:00')
        end=pd.to_datetime(end).strftime('%Y-%m-%d %H:%M:00')
        s = sql.read_sql('select fecha_hora,%s from calidad_agua where fecha_hora between "%s" and "%s" and calidad in (1,2) order by fecha_hora'%(variable,start,end)).set_index('fecha_hora')
        return s.loc[:,s.keys()[0]]

    def plot_time_series(self,series,start=None,end=None,show_rolling=False,rolling_rate=5,save_path=None,dpi=98,show_data=True):
        '''
        Plots a varible time series
        '''
        start=series.first_valid_index() if start==None else pd.to_datetime(start)
        end=series.last_valid_index() if end==None else pd.to_datetime(end)

        series=series[(series.index>start)&(series.index<end)&(series>0)].reindex(pd.date_range(start,end,freq='T'))
        fig=plt.figure(figsize=(10,6))
        ax=fig.add_subplot(111)
        if show_data:
            ax.plot(series.index,series.values,color=self.colores_siata[0],label='Serie de tiempo')
        n=len(series)

        if show_rolling:
            rolling=series.rolling(int(n*rolling_rate/100.),center=True).mean()
            ax.plot(rolling.index,rolling,'-',lw=2,color=self.colores_siata[3],label='Media movil')
            ax.legend(loc=(.25,-.25),ncol=2,prop=self.fontype,fontsize=20)

        ax.set_xlim(series.index.min(),series.index.max())
        ax.set_title(str(self.codigo_calidad)+' | '+self.info.Nombreestacion+' | '+'serie de tiempo '+series.name.upper(),fontsize=16,fontproperties=self.fontype)
        var_names={'do':u'Oxígeno disuelto [mg/L]','ph':'pH','orp':u'Potencial de oxido reducción [mV]','t':'Temperatura [C]'}

        ax.set_ylabel(var_names[series.name.lower()],fontsize=16,fontproperties=self.fontype)
        ax.xaxis.set_tick_params(labelsize=12)
        ax.yaxis.set_tick_params(labelsize=12)
        mx=series.max()
        mn=series.min()
        s=series.std()

        if show_data:
            if series.name.lower()=='ph':
                print(mx+s)
                ax.set_ylim(max(mn-s,0),min(mx+s,14))
                ymax_ticks=round(min(mx+s,14),0)
            else:
                ax.set_ylim(max(mn-s,0),mx+s)
                ymax_ticks=round(mx+s,0)

            yticks=np.linspace(round(max(mn-s,0),0),ymax_ticks,6)
            ax.set_yticks(yticks)
            ax.set_yticklabels(yticks,fontproperties=self.fontype,fontsize=15)

        xticks=series[::n//6].index

        ax.set_xticks(xticks)
        ax.set_xticklabels(xticks.strftime('%Y-%m-%d'),fontproperties=self.fontype,fontsize=15)
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M\n%d-%b-%y'))
        ax.grid(linestyle='--',lw=2,alpha=.15)

        if save_path!=None:
            plt.savefig(save_path,dpi=dpi,bbox_inches='tight')

        return ax


    def plot_mean(self,series,grouper=None,save_path=None,dpi=98):
        '''
        Plots a varible mean for the given time window, groupping data by the given step, by default it considers the minimun time step concerning the given window, i.e. hourly for 6h,12h,24h,48h and 72h; daily for weekly (1w,2w,3w) and monthly (1m) windows; weekly for months (2m,3m,...,12m); monthly for year plots ('1y') and yearly for "history" plots.

        -------inputs--------
        series: pandas time series containing the desired variable.
        window: time window containing the data to plot, if not set it's guessed
        step:   time delta to group data, if not set it's guessed from the time window

        '''

        start,end=series.first_valid_index(),series.last_valid_index()
        series=series[(series.index>start)&(series.index<end)&(series>0)].reindex(pd.date_range(start,end,freq='T'))

        if grouper==None:
            window=end-start
            grouper='1a' if window.days>365*3 else '1m' if window.days>365/2 else '1w' if window.days>92 else '1d' if window.days>3 else '1h'

        delta='horario' if grouper[-1]=='h' else 'diario' if grouper[-1]=='d' else 'semanal' if grouper[-1]=='w' else 'mensual' if grouper[-1]=='m' else 'anual' if grouper[-1]=='a' else ''
        width=(.8/24)*int(grouper[:-1]) if grouper[-1]=='h' else .8*int(grouper[:-1]) if grouper[-1]=='d' else (.8)*int(grouper[:-1])*7 if grouper[-1]=='w' else (.8)*int(grouper[:-1])*30 if grouper[-1]=='m' else (.8)*int(grouper[:-1])*365 if grouper[-1]=='a' else .8

        count=pd.Series(data=series.index,index=series.index).groupby(pd.TimeGrouper(grouper)).count()
        valid=series.dropna().groupby(pd.TimeGrouper(grouper)).count()
        percentage=valid/count.max()
        percentage[percentage<.5]=np.nan

        grouped=series.groupby(pd.TimeGrouper(grouper)).mean()
        grouped.loc[percentage.index[np.isnan(percentage)]]=np.nan

        fig=plt.figure(figsize=(10,6))
        ax=fig.add_subplot(111)
        ax.bar(grouped.index,grouped.values,color=self.colores_siata[0],label='Valor medio',width=width)
        n=len(grouped)
        fs=12 if n>12 else 14
        rot=90 if n>12 else 0
        rects = ax.patches

        ax.set_xlim(grouped.index.min(),grouped.index.max())
        ax.set_title(str(self.codigo_calidad)+' | '+self.info.Nombreestacion+' | '+'promedio '+delta+' '+series.name.upper(),fontsize=16,fontproperties=self.fontype)
        var_names={'do':u'Oxígeno disuelto [mg/L]','ph':'pH','orp':u'Potencial de oxido reducción [mV]','t':'Temperatura del agua [C]'}
        ax.set_ylabel(var_names[series.name.lower()],fontsize=16,fontproperties=self.fontype)
        ax.xaxis.set_tick_params(labelsize=12)
        ax.yaxis.set_tick_params(labelsize=12)


        if series.name.lower()=='ph':
            mx=14/1.3
        else:
            mx=grouped.max()

        ax.set_ylim(0,mx*1.3)
        ax.set_xlim(grouped.index[0]-self.parse_time('1'+grouper[-1]),grouped.index[-1]+self.parse_time('1'+grouper[-1]))

        xticks=grouped[::max(1,n//11)].index
        yticks=[round(x) for x in np.linspace(0,mx*1.3,6)]

        ax.set_xticks(xticks)
        ax.set_xticklabels(xticks,fontproperties=self.fontype,fontsize=12,rotation=90)
        ax.set_yticks(yticks)
        ax.set_yticklabels(yticks,fontproperties=self.fontype,fontsize=15)

        if grouper[-1]=='h':
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M %d-%b-%y'))
        elif grouper[-1]=='a':
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y'))
        else:
            ax.xaxis.set_major_formatter(mdates.DateFormatter('%d-%b-%y'))

        ax.grid(linestyle='--',lw=2,alpha=.15)

        self.autolabel(rects,ax,rot,fs)

        if save_path!=None:
            plt.savefig(save_path,dpi=dpi,bbox_inches='tight')

        return grouped,ax

    def histogram_variable(self,series,bins=10,bar_plot=False,fill=True,units='',var_name='',save_path=None,dpi=98):
        hist,bins=np.histogram(series,bins)
        hist=hist.astype(float)/hist.sum()
        fig=plt.figure(figsize=(12,7))
        ax=fig.add_subplot(111)

        if bar_plot:
            ax.bar((bins[:-1]/2.+bins[1:]/2.),hist,color=self.colores_siata[0],width=(max(bins)-min(bins))/float(len(bins)+3))
        elif fill:
            ax.plot((bins[:-1]/2.+bins[1:]/2.),hist,color=self.colores_siata[0])
            ax.fill_between((bins[:-1]/2.+bins[1:]/2.),hist,color=self.colores_siata[0])
        else:
            ax.plot((bins[:-1]/2.+bins[1:]/2.),hist,color=self.colores_siata[0],lw=3)

        series.name=var_name if var_name!='' else series.name.upper()

        ax.set_title(series.name+' data distribution',fontsize=15,fontproperties=self.fontype)
        ax.set_ylabel('Relative frecuency',fontsize=15,fontproperties=self.fontype)
        ax.set_xlabel(series.name+' value %s'%units,fontsize=15,fontproperties=self.fontype)
        ax.grid(linestyle='--',lw=2,alpha=.2)

        ax.axvline(series.quantile(.1),color=self.colores_siata[3],ls=':',lw=3,label='P10=%.2f'%series.quantile(.1),zorder=1)
        ax.axvline(series.quantile(.5),color=self.colores_siata[3],ls='-',lw=3,label='P50=%.2f'%series.quantile(.5),zorder=1)
        ax.axvline(series.quantile(.9),color=self.colores_siata[3],ls='--',lw=3,label='P90=%.2f'%series.quantile(.9),zorder=1)
        ax.plot(np.nan,np.nan,'.',label=r'$\sigma$=%.2f'%series.std(),color='none',markersize=0.1)
        ax.plot(np.nan,np.nan,'.',label=r'Max=%.2f'%series.max(),color='none')
        ax.plot(np.nan,np.nan,'.',label=r'Min=%.2f'%series.min(),color='none')
        ax.set_ylim(0,min(max(hist)*1.3,1))
        ax.legend(loc=(0,-.25),ncol=10,prop=self.legendfont)

        for xlabel in ax.get_xticklabels():
            xlabel.set_fontproperties(self.fontype)

        for ylabel in ax.get_yticklabels():
            ylabel.set_fontproperties(self.fontype)

        ax.xaxis.set_tick_params(labelsize=14)
        ax.yaxis.set_tick_params(labelsize=14)

        if save_path!=None:
            plt.savefig(save_path,dpi=dpi,bbox_inches='tight')

        return hist,bins,ax

    def diurnal_cycle(self,series,val='mean',units='',var_name='',vlim=None,save_path=None,dpi=98):
        fig=plt.figure(figsize=(12,7))
        ax=fig.add_subplot(111)

        series.name=var_name if var_name!='' else series.name.upper()

        if val=='mean':
            diurnal=series.groupby(lambda x:x.hour).mean()
            ax.set_title(series.name+' mean diurnal cycle',fontproperties=self.fontype,fontsize=15)

        else:
            try:
                diurnal=series.groupby(lambda x:x.hour).quantile(float(val.split('P')[-1])/100)
                ax.set_title(series.name+' %s diurnal cycle'%val,fontproperties=self.fontype,fontsize=15)
            except:
                print('Wrong value for argument val, valid values are "mean" and "P#", being # the number of desired percentile.')
                raise ValueError

        ax.plot(diurnal.index,diurnal,linestyle='-',lw=3,color=self.colores_siata[0])
        ax.plot(diurnal.index,diurnal,'o',lw=3,color=self.colores_siata[3])

        for xlabel in ax.get_xticklabels():
            xlabel.set_fontproperties(self.fontype)

        for ylabel in ax.get_yticklabels():
            ylabel.set_fontproperties(self.fontype)

        ax.xaxis.set_tick_params(labelsize=14)
        ax.yaxis.set_tick_params(labelsize=14)
        ax.set_ylabel(series.name+' value %s'%units,fontproperties=self.fontype,fontsize=15)
        ax.set_xlabel('Day hour',fontproperties=self.fontype,fontsize=15)
        ax.set_xticks(range(0,24,2))
        ax.grid(linestyle = '--',lw=2,alpha=.2)

        if vlim!=None:
            ax.set_ylim(min(vlim),max(vlim))
        else:
            ax.set_ylim(diurnal.mean()-diurnal.std()*3,diurnal.mean()+diurnal.std()*3)

        if save_path!=None:
            plt.savefig(save_path,dpi=dpi,bbox_inches='tight')

        return diurnal,ax

    @logger
    def watershed_delineation(self, latlon = None, codigo = None, zoom = 0.05, searh_radius = 0.01, dxp = 60, save = False, dem_dir_nodes_path = None):
        '''

        '''
        import scipy.spatial as spatial
        import wmf.wmf as wmf
        from mpl_toolkits.basemap import Basemap
        from matplotlib.patches import Polygon
        from matplotlib.collections import PatchCollection

        bck_codigo = self.codigo_calidad
        self.codigo_calidad = int(codigo) if codigo!=None else self.codigo_calidad

        if latlon==None:
            lat=float(self.info.latitude)
            lon=float(self.info.longitude)
        else:
            lat=latlon[0]
            lon=latlon[1]

        add=searh_radius
        add_out=zoom
        dt =300

        if dxp == 60:
            umbral = 1000
        else:
            umbral = 500

        if dem_dir_nodes_path==None:
            folder_path = info.DATA_PATH_CALIDAD_AGUA + 'basin_maker'
            dem_path = '%s/dem_%s.tif'%(folder_path,int(dxp))
            dir_path = '%s/dir_%s.tif'%(folder_path,int(dxp))
            nodes_path = '%s/nodes_%s.csv'%(folder_path,int(dxp))
            net_path = '%s/net_%s'%(folder_path,int(dxp))
        else:
            folder_path=dem_dir_nodes_path
            dem_path = '%s/dem_%s.tif'%(folder_path,int(dxp))
            dir_path = '%s/dir_%s.tif'%(folder_path,int(dxp))
            nodes_path = '%s/nodes_%s.csv'%(folder_path,int(dxp))
            net_path = '%s/net_%s'%(folder_path,int(dxp))


        #finding nearest point
        llcrnrlat=lat-add;urcrnrlat=lat+add;llcrnrlon=lon-add;urcrnrlon=lon+add
        df = pd.read_csv(nodes_path,index_col=0)
        bt = df[((df.index.values>(llcrnrlon))&(df.index.values<(urcrnrlon)))&((df['Y']>llcrnrlat)&(df['Y']<urcrnrlat))]
        A = map(lambda x,y: [x,y],bt.index,bt.Y.values)
        lon_found,lat_found = tuple(A[spatial.KDTree(A).query((lon,lat))[1]])
        #plot
        fig = plt.figure(figsize=(7,7))
        axis =fig.add_subplot(111)
        m = Basemap(projection='merc',llcrnrlat=llcrnrlat-add_out,urcrnrlat=urcrnrlat+add_out,llcrnrlon=llcrnrlon- add_out,urcrnrlon=urcrnrlon + add_out,resolution='l',ax=axis)
        m.readshapefile(net_path,'net')
        x,y = m(lon,lat)
        x3,y3 = m(lon_found,lat_found)
        x2,y2 = m(bt.index.values,bt['Y'].values)
        m.scatter(x,y,s=100,zorder = 20)
        m.scatter(x2,y2,s=50,color='tan',alpha=0.4)
        m.scatter(x3,y3,s=50,color='r')
        #setting wmf
        wmf.cu.nodata=-9999.0
        wmf.cu.dxp=dxp
        DEM = wmf.read_map_raster(dem_path,True,dxp=dxp)
        DIR = wmf.read_map_raster(dir_path,True,dxp=dxp)
        DEM[DEM==65535] = -9999
        DIR[DIR<=0] = wmf.cu.nodata.astype(int)
        DIR = wmf.cu.dir_reclass_rwatershed(DIR,wmf.cu.ncols,wmf.cu.nrows)
        st = wmf.Stream(lon_found,lat_found,DEM,DIR,name = 'Stream%s'%self.info.slug)
        cu = wmf.SimuBasin(lon_found,lat_found, DEM, DIR,name='Basin%s'%self.slug, dt = dt, umbral=umbral, stream=st)
        rute_shapes = '/media/nicolas/maso/calidad_agua/shapes'
        paths = {'stream_path':'%s/streams/%s'%(rute_shapes,self.slug),
                'net_path' : '%s/net/%s'%(rute_shapes,self.slug),
                'polygon_path' : '%s/polygon//%s'%(rute_shapes,self.slug)}

        for path in paths.values():
            os.system('mkdir %s'%path)

        nc_path = '/media/nicolas/maso/calidad_agua/basins/%s.nc'%self.slug
        stream_path=paths['stream_path']+'/'+self.slug
        net_path=paths['net_path']+'/'
        polygon_path=paths['polygon_path']+'/'

        st.Save_Stream2Map(stream_path)
        cu.Save_Net2Map(net_path)
        cu.set_Geomorphology()
        cu.GetGeo_Cell_Basics()
        cu.GetGeo_Parameters()
        cu.Save_Basin2Map(polygon_path,dx=dxp)
        cu.Save_SimuBasin(nc_path,ruta_dem = dem_path,ruta_dir = dir_path)
        params=cu.GeoParameters
        m.readshapefile(polygon_path+'layer1','basin',zorder=30,color='g')
        m.readshapefile(net_path+'layer1','net',zorder=50,color='red')

        for path in [polygon_path,net_path,stream_path]:
            os.system('zip -rj %s %s'%(path+self.slug+'.zip',path+'layer1*'))

        patches = []
        for info, shape in zip(m.basin, m.basin):
            patches.append( Polygon(np.array(shape), True))
            axis.add_collection(PatchCollection(patches, facecolor= self.colores_siata[0], edgecolor=self.colores_siata[6], linewidths=0.5, zorder=30,alpha=0.6))

        if save:
            self.modify_station_data(self.codigo_calidad,area=round(params['Area[km2]'],2),nc_path=nc_path,net_path=net_path+'layer1',stream_path=stream_path,polygon_path=polygon_path+'layer1',slug=self.slug)

        self.codigo_calidad=bck_codigo
