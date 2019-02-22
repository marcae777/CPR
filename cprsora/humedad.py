#!/usr/bin/env python
# -*- coding: utf-8 -*-

import numpy as np
import pandas as pd
import pylab as pl
import json
import matplotlib.font_manager as fm

# old versions, pending for change
import cprv1 as cprv1
from cpr import cpr
import information as info


import sys
# reload(sys)
# sys.setdefaultencoding("utf-8")

def plot_HydrologicalVar(dfax,dfax2,ylabelax,ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                         path_fuentes,colors,window,bottomtext,ylocfactor_texts,yloc_legends,rutafig=None,
                         loc2legend=None,title=None):
    '''
    Make a df.plot with parallel axisa and the SIATA format, It's useful for plotting rainfall.
    Can be setted for plotting .png's ready for SIATA webpage.

    loc2legend: If None returns a plot for webpage, if not None have to be the location (x,y) for ax.legend

    --------
    Returns:
    - Plot

    '''
    #properties
    fonttype = fm.FontProperties(fname=path_fuentes)
    pl.rc('axes',labelcolor='#4f4f4f')
    pl.rc('axes',linewidth=1.25)
    pl.rc('axes',edgecolor='#4f4f4f')
    pl.rc('text',color= '#4f4f4f')
    pl.rc('text',color= '#4f4f4f')
    pl.rc('xtick',color='#4f4f4f')
    pl.rc('ytick',color='#4f4f4f')
    fonttype.set_size(9.5)
    legendfont=fonttype.copy()
    legendfont.set_size(12)

    #figure
    fig  =pl.figure(dpi=120,facecolor='w')
    ax = fig.add_subplot(111)
    dfax.plot(ax=ax,lw=1.85,color=colors[:-1])
    pl.yticks(fontproperties=fonttype)
    pl.xticks(fontproperties=fonttype)
    ax.set_ylabel(ylabelax,fontproperties=fonttype,fontsize=fontsizeylabel)
    ax.set_xlabel(xlabel,fontproperties=fonttype,fontsize=fontsizexlabel)
    if title is not None:
        ax.set_title(title,fontproperties=fonttype,fontsize=fontsizexlabel)
    #second axis
    axAX=pl.gca()
    ax2=ax.twinx()
    ax2AX=pl.gca()
    dfax2.plot(ax=ax2,alpha=0.5,color=[colors[-1],colors[-1]])
    
    ax2.set_ylim(0,ax2AX.get_ylim()[1]*2.5)
    ax2AX.set_ylim(ax2AX.get_ylim() [::-1]) 
    ax2.set_ylabel(ylabelax2,fontproperties=fonttype,fontsize=fontsizeylabel)
    pl.yticks(fontproperties=fonttype)

    if loc2legend is None: 
        #setting loc's
        if window == '3 hours':
            yloc_legend = yloc_legends[0]
            ylocfactor_text=ylocfactor_texts[0]
        elif window == '24 hours' or window == '72 hours':
            yloc_legend = yloc_legends[1]
            ylocfactor_text=ylocfactor_texts[1]
        else:
            yloc_legend = yloc_legends[2]
            ylocfactor_text = ylocfactor_texts[2]
        #legend
        ax.legend(loc=(0.15,yloc_legend),ncol=2,prop=legendfont)
        #se ubica el text, x e y que se ajustan de acuerdo a los dominios de x e y
        ax.text(dfax.index[int(dfax.shape[0]*0.083)], ax.get_ylim()[0]-(1*(ax.get_ylim()[1]-ax.get_ylim()[0])*ylocfactor_text),
                bottomtext, fontsize=11.5, fontproperties=fonttype,
                bbox=dict(edgecolor='#c1c1c1',facecolor='w'))
    else:
        ax.legend(loc=loc2legend,ncol=2,prop=legendfont)
    if rutafig is not None:
        pl.savefig(rutafig,bbox_inches='tight',dpi=120,facecolor='w')


class Humedad(cprv1.SqlDb):
    '''
    Provide functions to manipulate data related
    to soil moisture sensors.
    '''
    local_table  = 'estaciones_estaciones'
    remote_table = 'estaciones'
    
    def __init__(self,codigo=None,**kwargs):
        '''
        The instance inherits modules to manipulate SQL
        Parameters
        ----------
        codigo        : primary key
        remote_server :
        local_server  : database kwargs to pass into the Sqldb class
        nc_path       : path of the .nc file to set wmf class
        '''
        self.data_path ='/media/nicolas/maso/Mario/'
        cprv1.SqlDb.__init__(self,**info.LOCAL)
        
        self.colores_siata = [[0.69,0.87,0.93],[0.61,0.82,0.88],[0.32,0.71,0.77],[0.21,0.60,0.65],#\
                                  [0.0156,0.486,0.556],[0.007,0.32,0.36],[0.0078,0.227,0.26]]
        self.colores_siata2 = ['#1487B9', '#22467F','#09202E','#004D56',\
                              '#70AFBA','#98D1DD','#8ABB73','#C7D15D']
        self.colores_siata_sora=['#008d8d','#3CB371']
        
        self.colores_random=['#1487B9','#8ABB73',' #22467F',' #C7D15D',' #004D56',' #3CB371']

        self.best_color=['#C7D15D','#3CB371', '#22467F']
        
        self.local_server = info.LOCAL
        self.remote_server1 = info.REMOTE
        self.remote_server2 = info.REMOTE_h
        self.codigo = codigo
        
#     @property
#     def info_remote(self):
#         query = "SELECT * FROM %s WHERE red = 'humedad' and codigo='%s'"%(self.remote_table,self.codigo)
#         s = cprv1.SqlDb(**self.remote_server1).read_sql(query).T
#         return s[s.columns[0]]
    
    @property
    def info(self):
        query = "SELECT * FROM %s WHERE clase = 'H' and codigo='%s'"%(self.local_table,self.codigo)
        s = self.read_sql(query).T
        return s[s.columns[0]]

#     @property
#     def infost_remote(self):
#         '''
#         Gets full information from all stations
#         Returns
#         ---------
#         pd.DataFrame
#         '''
#         query = "SELECT * FROM %s WHERE red ='humedad'"%(self.remote_table)
#         return cprv1.SqlDb(**self.remote_server1).read_sql(query).set_index('Codigo')
    
    @property
    def infost(self):
        '''
        Gets full information from all stations
        Returns
        ---------
        pd.DataFrame
        '''
        query = "SELECT * FROM %s WHERE clase ='H'"%(self.local_table)
        return self.read_sql(query).set_index('codigo')

    def update_sql(self,table,field,value,pk):
        query = "update %s set %s = %s where id = %s"%(table,field,value,pk)
        self.execute_sql(query)
        
    def update_estado(self,codigo):
        table = 'estaciones_estaciones'
        field = 'estado'
        value = "'"+cpr.SqlDb(**self.remote_server1).read_sql("select estado from estaciones where codigo = '%s'"%(codigo)).values[0][0]+"'"
        pk = self.infost.id.loc[codigo]
        self.update_sql(table,field,value,pk)
    
    def read_humedad(self,start,end,server):
        '''
        Read soil moisture data from SIATA server - SAL
        
        Returns
        ---------
        pd.DataFrame
        '''
        start,end =  pd.to_datetime(start),pd.to_datetime(end)
        s = cprv1.SqlDb(**server).read_sql("select fecha_hora, h1, h2, h3, c1, c2, c3, t1, t2, t3, vw1, vw2, vw3,calidad,source from humedad_rasp where cliente = '%s' and fecha_hora between '%s' and '%s'"%(self.codigo,start,end)).set_index('fecha_hora')
        s = s.loc[s.index.dropna()]
        s[s<0.0] = np.NaN
        s=s.reindex(pd.date_range(start.strftime('%Y-%m-%d %H:%M'),end.strftime('%Y-%m-%d %H:%M'),freq='1T'))
        
        return s
    
    def plot_Humedad2Webpage(self,start,end,pluvio_s,ruta_figs):#,rutacredentials_remote,rutacredentials_local)
        '''
        Execute the operational plots of the SIATA soil moisture network within the official webpage format,
        colors, legends, time windows, etc. Use self.read_humedad() and plot_HydrologicalVar() functions for DB querys
        and plotting.
        
        Returns
        ---------
        Any returns besides the plots.
        '''
        # Consulta SAL - Humedad
        soilm_df=self.read_humedad(start,end,self.remote_server1)

        # Se escoge info y graficas de acuerdo al tipo de sensor.
        if self.info.get('tipo_sensor') == 1:
            soilm_df=soilm_df[soilm_df.columns[3:-2]]
            tiposensor='Digitales'
            soilm_df.columns=['Sensor CE 1','Sensor CE 2','Sensor CE 3','Sensor T 1','Sensor T 2','Sensor T 3','Sensor CVA 1','Sensor CVA 2','Sensor CVA 3']
            #Set df
            soilm_df[['p1','p2']]=pluvio_s[['p1','p2']]
            #plot
            yloc_legends=[-0.34,-0.39,-0.46]
            ylocfactor_texts=[0.88,0.925,0.98]    
            title=str(self.info.get('codigo'))+' | '+self.info.get('nombre')
            dfaxs=[soilm_df[soilm_df.columns[:3]],soilm_df[soilm_df.columns[3:6]],soilm_df[soilm_df.columns[6:9]]]
            dfax2=soilm_df[soilm_df.columns[-1]]
            ylabelaxs=['Conductividad Eléctrica   $(dS.m^{-1})$', u'Temperatura ($^\circ$C)','Cont. Volumétrico de  Agua $(\%)$']
            ylabelax2='Precipitación  ($mm$)'
            xlabel='Tiempo'
            fontsizeylabel=13.5
            fontsizexlabel=16
            path_fuentes='/media/nicolas/Home/Jupyter/Sebastian/AvenirLTStd-Book/AvenirLTStd-Book.otf'
            window=str(int((end-start).total_seconds()/3600))+' hours'
            loc2legend=None
            colors=[self.colores_siata_sora[1],self.colores_siata_sora[0],self.colores_siata2[2],self.colores_siata2[4]]
            #cant. de datos qe llegan sobre los esperados
            perc_datos= round((dfaxs[0].dropna().shape[0]/float(dfaxs[0].shape[0]))*100,2)
            bottomtext= 'Esta estación tiene tres sensores a 0.1, 0.5 y 0.9 metros\nde profundidad. Cada uno mide Contenido Volumétrico\nde Agua (CVA), Temperatura (T) y Conductividad Eléctri-\nca (CE) del suelo. También cuenta con una estación plu-\nviométrica asociada.\n \nTipo de Sensor: '+tiposensor+'\nResolución Temporal: 1 min. \nPorcentaje de datos transmitidos*: '+str(perc_datos)+u'% \n *Calidad de datos aun sin verificar exhaustivamente.'
            rutafig=ruta_figs+str(int((end-start).total_seconds()/3600))+'_hours/'+str(self.info.get('codigo'))
            namesfig=['EC','T','VW']
            rutafigs= [rutafig+'_'+namefig+'.png' for namefig in namesfig]
            for index,dfax in enumerate(dfaxs):
                plot_HydrologicalVar(dfax,dfax2,ylabelaxs[index],ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                            path_fuentes,colors,window,bottomtext,ylocfactor_texts,yloc_legends,rutafigs[index],title=title)

        else:
            soilm_df=soilm_df[soilm_df.columns[:3]]
            tiposensor='Análogos'
            soilm_df.columns=['Sensor CVA 1','Sensor CVA 2','Sensor CVA 3']
            #Set df
            soilm_df[['p1','p2']]=pluvio_s[['p1','p2']]
            #plot
            yloc_legends=[-0.34,-0.395,-0.455]
            ylocfactor_texts=[0.83,0.88,0.94]
            title=str(self.info.get('codigo'))+' | '+self.info.get('nombre')
            dfax=soilm_df[soilm_df.columns[:-1]]
            dfax2=soilm_df[soilm_df.columns[-1]]
            ylabelax='Cont. Volumétrico de  Agua $(\%)$'
            ylabelax2='Precipitación  ($mm$)'
            xlabel='Tiempo'
            fontsizeylabel=13.5
            fontsizexlabel=16
            path_fuentes='/media/nicolas/Home/Jupyter/Sebastian/AvenirLTStd-Book/AvenirLTStd-Book.otf'
            window=str(int((end-start).total_seconds()/3600))+' hours'
            loc2legend=None
            colors=[self.colores_siata_sora[1],self.colores_siata_sora[0],self.colores_siata2[2],self.colores_siata2[4]]
            #cant. de datos qe llegan sobre los esperados
            perc_datos= round((dfax.dropna().shape[0]/float(dfax.shape[0]))*100,2)
            bottomtext= 'Esta estación tiene tres sensores a 0.1, 0.5 y 0.9 metros\nde profundidad que miden el Contenido Volumétrico de\nAgua (CVA) en el suelo, también cuenta con una esta\nción pluviométrica asociada.\n \nTipo de Sensores: '+tiposensor+'\nResolución Temporal: 1 min. \nPorcentaje de datos transmitidos*: '+str(perc_datos)+'% \n *Calidad de datos aun sin verificar exhaustivamente.'
            rutafig=ruta_figs+str(int((end-start).total_seconds()/3600))+'_hours/'+str(self.info.get('codigo'))+'_H.png'
            plot_HydrologicalVar(dfax,dfax2,ylabelax,ylabelax2,xlabel,fontsizeylabel,fontsizexlabel,
                                     path_fuentes,colors,window,bottomtext,ylocfactor_texts,yloc_legends,rutafig,title=title)