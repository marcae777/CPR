#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  CRP.py
#
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>

import pandas as pd
import numpy as np

class Hidraulica(Nivel):
    def __init__(self,**kwargs):
        Nivel.__init__(self,**kwargs)
        self.info = pd.Series(index=Nivel(codigo=99,user='mario',passwd='mc@n0Yw2E').info.copy().index)
        self.info.slug = self.info_redrio.loc[self.codigo,'Nombre'].decode('utf8').encode('ascii', errors='ignore').replace('.','').replace(' ','').replace('(','').replace(')','')
        self.fecha = '2006-06-06 06:06'
        self.workspace = '/media/nicolas/Home/Jupyter/MarioLoco/repositories/CPR/documentacion/redrio/'
        self.seccion = pd.DataFrame(columns = [u'vertical', u'x', u'y', u'v01', u'v02', u'v03', u'v04', u'v05', u'v06', u'v07', u'v08', u'v09', u'vsup'])
        self.parametros = "id_aforo,fecha,ancho_superficial,caudal_medio,velocidad_media,perimetro,area_total,altura_media,radio_hidraulico"
        self.aforo = pd.Series(index = [u'fecha', u'ancho_superficial', u'caudal_medio', u'velocidad_media',u'perimetro', u'area_total',
     u'altura_media', u'radio_hidraulico',u'levantamiento'])
        self.info.nc_path = '/media/nicolas/maso/Mario/basins/%s.nc'%self.codigo
        self.info.nombre = self.info_redrio.loc[self.codigo,'Nombre']
        self.info.longitud = self.info_redrio.loc[self.codigo,'Longitud']
        self.info.latitud = self.info_redrio.loc[self.codigo,'Latitud']
        self.levantamiento = pd.DataFrame(columns = ['vertical','x','y'])
        self.alturas = pd.DataFrame(index=pd.date_range(start = pd.to_datetime('2018').strftime('%Y-%m-%d 06:00'),periods=13,freq='H'),columns = ['profundidad','offset','lamina','caudal'])
        self.alturas.index = map(lambda x:x.strftime('%H:00'),self.alturas.index)

    @property
    def info_redrio(self):
        return pd.read_csv('/media/nicolas/Home/Jupyter/MarioLoco/redrio/info_redrio.csv',index_col=0)

    @property
    def caudales(self):
        return self.aforos().set_index('fecha')['caudal']

    @property
    def folder_path(self):
        return self.workspace+pd.to_datetime(self.fecha).strftime('%Y%m%d')+'/'+self.info.slug+'/'

    @property
    def aforo_nueva(self):
        pass

    @property
    def seccion_aforo_nueva(self):
        pass

    @property
    def levantamiento_aforo_nueva(self):
        pass

    def get_levantamiento(self,id_aforo):
        seccion = self.read_sql("SELECT * FROM levantamiento_aforo_nueva WHERE id_aforo = '%s'"%(id_aforo)).set_index('vertical')
        return seccion[['x','y']].sort_index()

    def aforos(self,filter=True):
        aforos = self.read_sql("SELECT %s from aforo_nueva where id_estacion_asociada = '%s'"%(self.parametros,self.codigo))
        aforos = aforos.set_index('id_aforo')
        if filter:
            aforos[aforos==-999]=np.NaN
        aforos = aforos.dropna()
        aforos = aforos.sort_values('fecha')
        aforos['levantamiento']=False
        for id_aforo in aforos.index:
            if self.get_levantamiento(id_aforo).index.size:
                aforos.loc[id_aforo,'levantamiento'] = True
        return aforos
    @property
    def levantamientos(self):
        return self.aforos(filter=False)[self.aforos(filter=False)['levantamiento']].index

    def insert_vel(self,vertical,v02,v04,v08):
        self.seccion.loc[vertical,'v02'] = v02
        self.seccion.loc[vertical,'v04'] = v04
        self.seccion.loc[vertical,'v08'] = v08

    def velocidad_media_dovela(self):
        columns = [u'vertical', u'x', u'y', u'v01', u'v02', u'v03', u'v04', u'v05', u'v06', u'v07', u'v08', u'v09', u'vsup']
        dfs = self.seccion[columns].copy()
        self.seccion['vm'] = np.NaN
        vm = []
        for index in dfs.index:
            vm.append(round(self.estima_velocidad_media_vertical(dfs.loc[index].dropna()),3))
        self.seccion['vm'] = vm
    def area_dovela(self):
        self.seccion['area'] = self.get_area(self.seccion['x'].abs().values,self.seccion['y'].abs().values)

    def estima_velocidad_media_vertical(self,vertical,factor=0.0,v_index=0.8):
        vertical = vertical[vertical.index!='vm']
        index = list(vertical.index)
        if index == ['vertical','x','y']:
            if vertical['x'] == 0.0:
                vm = factor * self.seccion.loc[vertical.name+1,'vm']
            else:
                vm = factor * self.seccion.loc[vertical.name-1,'vm']
        elif (index == ['vertical','x','y','vsup']) or (index == ['vertical','x','y','v08']):
            try:
                vm = v_index*vertical['vsup']
            except:
                vm = v_index*vertical['v08']
        elif (index == ['vertical','x','y','v04']) or (index == ['vertical','x','y','v04','vsup']):
            vm = vertical['v04']
        elif index == (['vertical','x','y','v04','v08']) or index == (['vertical','x','y','v04','v08','vsup'])  :
            vm = (2*vertical['v04']+vertical['v08'])/3.0
        elif index == ['vertical','x','y','v08','vsup']:
            vm = v_index*vertical['vsup']
        elif (index == ['vertical','x','y','v02','v04','v08']) or (index == ['vertical','x','y','v02','v04','v08','vsup']):
            vm = (2*vertical['v04']+vertical['v08']+vertical['v02'])/4.0
        return vm

    def perimetro(self):
        x,y = (self.seccion['x'].values,self.seccion['y'].values)
        def perimetro(x,y):
            p = []
            for i in range(len(x)-1):
                p.append(float(np.sqrt(abs(x[i]-x[i+1])**2.0+abs(y[i]-y[i+1])**2.0)))
            return [0]+p
        self.seccion['perimetro'] = perimetro(self.seccion['x'].values,self.seccion['y'].values)

    def get_area(self,x,y):
        '''Calcula las áreas y los caudales de cada
        una de las verticales, con el método de mid-section
        Input:
        x = Distancia desde la banca izquierda, type = numpy array
        y = Produndidad
        v = Velocidad en la vertical
        Output:
        area = Área de la subsección
        Q = Caudal de la subsección
        '''
        # cálculo de áreas
        d = np.absolute(np.diff(x))/2.
        b = x[:-1]+d
        area = np.diff(b)*y[1:-1]
        area = np.insert(area, 0, d[0]*y[0])
        area = np.append(area,d[-1]*y[-1])
        area = np.absolute(area)
        # cálculo de caudal
        return area

    def plot_compara_historicos(self,**kwargs):
        s = kwargs.get('s',self.aforos()['caudal_medio'])
        filepath = self.folder_path+'historico.png'
        caudal = self.aforo.caudal_medio
        xLabel = r"Caudal$\ [m^{3}/s]$"
        formato = 'png'
        fig = plt.figure(figsize=(14,4.15))
        ax1 = plt.subplot(121)
        ##CUMULATIVE
        ser = s.copy()
        ser.index = range(ser.index.size)
        p25 = s.quantile(0.25)
        p75 = s.quantile(0.75)
        ser.loc[ser.index[-1]+1] = p25
        ser.loc[ser.index[-1]+1] = p75
        ser = ser.sort_values()
        cum_dist = np.linspace(0.,1.,len(ser))
        ser_cdf = pd.Series(cum_dist, index=ser)
        lw=4.0
        ax = ax1.twinx()
        ser_cdf = ser_cdf*100
        ser_cdf.plot(ax=ax,color='orange',drawstyle='steps',label='',lw=lw)
        ser_cdf[ser_cdf<=25].plot(ax = ax,color='g',drawstyle='steps',label='Caudales bajos',lw=lw)
        ser_cdf[(ser_cdf>=25)&(ser_cdf<=75)].plot(ax=ax,color='orange',drawstyle='steps',label='Caudales medios',lw=lw)
        ser_cdf[ser_cdf>=75].plot(ax=ax,color='r',drawstyle='steps',label='Caudales altos',lw=lw)
        #ax.legend(fontsize=14,bbox_to_anchor=(0.5,-0.3),ncol=1)
        #ax.set_title('')
        ax.set_ylabel('Probabilidad [%]',fontsize=16)
        ax.grid()
        ax.set_xlim(0,s.max()*1.05)
        s.hist(ax = ax1,color=self.colores_siata[0],grid=False,bins=20,label='Histograma')
        ax2 = plt.subplot(122)
        ax1.axvline(caudal,color=self.colores_siata[-1],\
                    zorder=40,linestyle='--',\
                    label='Observado',lw=3.0)

        h1, l1 = ax1.get_legend_handles_labels()
        h2, l2 = ax.get_legend_handles_labels()
        ax1.legend(h1+h2, l1+l2, bbox_to_anchor=(1.45,-0.3),ncol=3,fontsize=14)
        ax1.set_ylabel('Frecuencia',fontsize = 16)
        ax1.set_xlabel(xLabel,fontsize = 16)
        for j in ['top','right']:
            ax1.spines[j].set_edgecolor('white')
        for j in ['top','right']:
            ax2.spines[j].set_edgecolor('white')
        ch = s.describe()[1:]
        ch.index = ['Media','Std.','Min.','P25','P50','P75','Max.']
        ch.loc['Obs.'] = caudal
        ch.sort_values().plot(kind = 'barh',ax=ax2,color=tuple(self.colores_siata[-2]),legend=False)
        ax2.yaxis.axes.get_yticklines() + ax2.xaxis.axes.get_xticklines()
        for pos in ['bottom','left']:
            ax2.spines[pos].set_edgecolor(self.colores_siata[-3])
        for pos in ['top', 'right']:
            ax2.spines[pos].set_edgecolor('white')
        size = s.index.size
        #plt.suptitle('Aforos históricos, número de datos = %s'%size,x=0.5,y = 1.05,fontsize=16)
        #ax1.set_title(u'a) Histograma aforos históricos',fontsize = 16)
        ax1.set_title('')
        for p in ax2.patches:
            ax2.annotate('%.2f'%p.get_width(), (p.get_width()*1.04,p.get_y()*1.02),va='bottom',fontsize=16)
        #ax2.set_title(u'b) Resumen aforos históricos',fontsize=16)
        ax2.set_xlabel(xLabel,fontsize = 16)
        import matplotlib as mpl
        mpl.rcParams['xtick.labelsize'] = 16
        mpl.rcParams['ytick.labelsize'] = 16
        plt.tight_layout()
        text = []
        labels = ax1.get_xticklabels()
        for label in labels:
            text.append(label.get_text())
        ax2.xaxis.set_ticks(map(lambda x:round(x,2),np.linspace(s.min(),s.max(),4)))
        plt.savefig(filepath,format = formato,bbox_inches = 'tight')

    def read_excel_format(self,file):
        df = pd.read_excel(file)
        df = df.loc[df['x'].dropna().index]
        df['vertical'] = range(1,df.index.size+1)
        df['y'] = df['y'].abs()*-1
        df.columns = map(lambda x:x.lower(),df.columns)
        self.seccion = df[self.seccion.columns]
        df = pd.read_excel(file,sheetname=1)
        fecha = pd.to_datetime(df.iloc[1].values[1])
        hora = pd.to_datetime(df.iloc[2].values[1])
        self.aforo.fecha = fecha.strftime('%Y-%m-%d')+hora.strftime(' %H:%M')
        self.aforo['x_sensor'] = df.iloc[4].values[1]
        self.aforo['lamina'] = df.iloc[5].values[1]
        df = pd.read_excel(file,sheetname=2)
        self.levantamiento = df[df.columns[1:]]
        self.levantamiento.columns = ['x','y']
        self.levantamiento.index.name = 'vertical'
        self.aforo.levantamiento = True


    def plot_lluvia_redrio(self,rain,rain_vect,filepath=None):
        fig = plt.figure(figsize=(20,8))
        # lluvia promedio
        ax1 = fig.add_subplot(121)
        ax1.set_ylabel('Intensidad (mm/h)')
        #ax1.set_title('%s - %s'%(rain.argmax(),rain.max()))
        ax1.spines['top'].set_color('w')
        ax1.spines['right'].set_color('w')
        ax1.set_title('Máxima intensidad: {0} - {1}'.format(self.maxint.split(',')[1].split(':')[1],':'.join(self.maxint.split(',')[2].split(':')[1:])))
        rain.plot(ax=ax1,linewidth=1,color='grey') # plot
        ax1.fill_between(rain.index,0,rain.values,facecolor=self.colores_siata[3])
        # lluvia acumulada
        ax2 = fig.add_subplot(122)
        ax2.set_title('Lluvia acumulada')
        self.rain_area_metropol(rain_vect.sum().values/1000.,ax=ax2)
        if filepath:
            plt.savefig(filepath,bbox_inches='tight')

    def plot_bars(self,s,filepath=None,bar_fontsize=14,decimales=2,xfactor =1.005,yfactor=1.01,ax=None):
        if ax is None:
            plt.figure(figsize=(20,6))

        s.plot(kind='bar',ax=ax)
        ax.set_ylim(s.min()*0.01,s.max()*1.01)
        for container in ax.containers:
                  plt.setp(container, width=0.8)
        for p in ax.patches:
            ax.annotate(str(round(p.get_height(),decimales)),
                        (p.get_x() * xfactor, p.get_height() * yfactor),
                        fontsize = bar_fontsize)
        for j in ['top','right']:
            ax.spines[j].set_edgecolor('white')
        ax.set_ylabel(r'$Caudal\ [m^3/s]$')
        if filepath:
            plt.savefig(filepath,bbox_inches='tight')

    def plot_curvas(self,filepath=None):
        fig = plt.figure(figsize=(14,20))
        for i,j in zip(range(1,5),['perimetro','area_total','altura_media','radio_hidraulico']):
            ax = fig.add_subplot(4,2,i)
            ax.scatter(self.aforos.dropna()['caudal_medio'].values,self.aforos.dropna()[j].values)
            ax.scatter(self.aforos.dropna().iloc[-1]['caudal_medio'],self.aforos.dropna().iloc[-1][j])
            ax.set_xlabel('Caudal')
            ax.set_ylabel(j)
        if filepath:
            plt.savefig(filepath,bbox_inches='tight')

    def plot_lluvia(self):
        # entrada
        #paths
        folder_path = pd.to_datetime(self.fecha).strftime('%Y%m%d')
        filepath = self.workspace+folder_path+'/%s'%self.info.slug+'/lluvia.png'
        # dates
        fecha = pd.to_datetime(self.fecha).strftime('%Y%m%d')
        end = pd.to_datetime(fecha)+datetime.timedelta(hours=18)
        start = end - datetime.timedelta(hours=(18+24-6))
        rain = self.radar_rain(start,end)*12.# convert hourly rain (intensity (mm/h))
        rain_vect = self.radar_rain_vect(start,end)
        self.maxint='fecha:%s,maximo:%s mm/h,fecha maximo:%s'%(fecha,rain.max(),rain.argmax())

        if len(rain_vect)>0:
            self.plot_lluvia_redrio(rain,rain_vect,filepath=filepath)
        elif len(rain_vect)==0:
            rain[0]=0.0001
            rain_vect=pd.DataFrame(np.zeros(self.ncells)).T
            self.plot_lluvia_redrio(rain,rain_vect,filepath=filepath)

    def plot_levantamientos(self):
        for id_aforo in self.levantamientos:
            self.plot_section(self.get_levantamiento(id_aforo),x_sensor=2,level=0.0)
            plt.title("%s : %s,%s"%(self.info.slug,self.codigo,id_aforo))

    def procesa_aforo(self):
        self.velocidad_media_dovela()
        self.area_dovela()
        self.seccion['caudal'] = self.seccion.vm*self.seccion.area
        self.perimetro()
        self.aforo.caudal_medio = self.seccion.caudal.sum()
        self.aforo.area_total = self.seccion.area.sum()
        self.aforo.velocidad_media = self.aforo.caudal_medio/self.aforo.area_total
        self.aforo.ancho_superficial = self.seccion['x'].abs().max()-self.seccion['x'].abs().min()
        self.aforo.perimetro = self.seccion.perimetro.sum()
        self.aforo.altura_media = self.seccion['y'].abs()[self.seccion['y'].abs()>0.0].mean()
        self.aforo.radio_hidraulico = self.aforo.area_total/self.aforo.perimetro
        self.fecha = self.aforo.fecha

    def plot_seccion(self):
        self.ajusta_levantamiento()
        self.plot_section(self.levantamiento,xSensor = self.aforo.x_sensor,level=self.aforo.lamina,fontsize=20)
        ax = plt.gca()
        plt.rc('font', **{'size':20})
        ax.scatter(self.aforo.x_sensor,self.aforo.lamina,marker='v',color='k',s=30+30,zorder=22)
        ax.scatter(self.aforo.x_sensor,self.aforo.lamina,color='white',s=120+30+10,edgecolors='k')
        ax.legend()
        ax.set_ylabel('Profundidad [m]')
        ax.spines['top'].set_color('w')
        ax.spines['right'].set_color('w')
        plt.savefig(self.folder_path+'seccion.png',bbox_inches='tight')

    def ajusta_levantamiento(self):
        cond = (self.levantamiento['x']<self.aforo.x_sensor).values
        flag = cond[0]
        for i,j in enumerate(cond):
            if j==flag:
                pass
            else:
                point = (tuple(self.levantamiento.iloc[i-1].values),tuple(self.levantamiento.iloc[i].values))
            flag = j
        intersection = self.line_intersection(point,((self.aforo.x_sensor,0.1*self.levantamiento['y'].min()),(self.aforo.x_sensor,1.1*self.levantamiento['y'].max(),(self.aforo.x_sensor,))))
        self.levantamiento = self.levantamiento.append(pd.DataFrame(np.matrix(intersection),index=['self.aforo.x_sensor'],columns=['x','y'])).sort_values('x')
        self.levantamiento['y'] = self.levantamiento['y']-intersection[1]
        self.levantamiento.index = range(1,self.levantamiento.index.size+1)
        self.levantamiento.index.name = 'vertical'

    def procesa_horarios(self):
        df_alturas = pd.DataFrame(index=self.alturas.index,columns=self.seccion.vertical)
        df_areas = df_alturas.copy()
        df_caudales = df_areas.copy()
        diferencias = self.alturas['profundidad']-self.alturas.loc[pd.to_datetime(self.aforo.fecha).strftime('%H:00'),'profundidad']
        for count,dif in enumerate(diferencias.values):
            alturas = (self.seccion['y'].abs()+dif).values
            alturas[alturas<=0.0] = 0.0
            area = self.get_area(self.seccion['x'].values,alturas)
            caudal = area*self.seccion['vm'].values
            df_alturas.iloc[count] = alturas
            df_areas.iloc[count] = area
            df_caudales.iloc[count] = caudal
        self.h_horaria = df_alturas
        self.a_horaria = df_areas
        self.q_horaria = df_caudales
        self.alturas['caudal'] = self.q_horaria.sum(axis=1).values

    def to_excel(self):
        from pandas import ExcelWriter
        excel_filepath = self.folder_path+'resultado.xlsx'
        writer =  ExcelWriter(excel_filepath)
        informacion = self.aforo.append(self.info_redrio.loc[self.codigo].iloc[:-1].drop('FolderName')).copy()
        try:
            informacion['Subcuenca'] = unicode(informacion['Subcuenca'],errors='ignore')
        except:
            pass
        informacion.to_excel(writer,'informacion',header=False, encoding='utf8')
        workbook  = writer.book
        worksheet = writer.sheets['informacion']
        worksheet.set_column('A:B', 20)
        self.seccion.set_index('vertical').to_excel(writer,'seccion', encoding='utf8')
        self.levantamiento.to_excel(writer,'levantamiento', encoding='utf8')
        self.alturas.index.name = 'Hora'
        self.alturas.fillna('').to_excel(writer,'caudales_horarios', encoding='utf8')
        workbook  = writer.book
        worksheet = writer.sheets['caudales_horarios']
        worksheet.set_column('B:B', 15)
        try:
            self.alturas.to_excel(writer,'profundidades_reportadas')
            self.h_horaria.to_excel(writer,'h_horaria')
            self.a_horaria.to_excel(writer,'a_horaria')
            self.q_horaria.to_excel(writer,'q_horaria')
        except:
            print ('no hourly data')
            pass
        writer.save()

    def get_num_pixels(self,filepath):
        import Image
        width, height = Image.open(open(filepath)).size
        return width,height

    def pixelconverter(self,filepath,width = False,height=False):
        w,h = self.get_num_pixels(filepath)
        factor = float(w)/h
        if width != False:
            return width/factor
        else:
            return height*factor

    def redrioreport(self,nombre_archivo,nombreEstacion,texto1,texto2,seccion,alturas,lluvia,histograma,resultados,fecha=None,numero_aforos=0,foot=None,head=None,estadisticas=False,heights=True,page2=True,table=True,one_page=False,**kwargs):
        '''
        Generates the reportlab reports of each station included in the attachtments.
        Parameters
        ----------
        nombre_archivo   = path where the pdf report will be generated.
        nombreEstacion   = station name that is going to be used as title of he report.
        texto1           = path to the plain tex file containing the first paragraph of the report which correspond to the descripiton of the registered levels trought the day and the station tranversal section.
        texto2           = path to the plain tex file containing the second paragraph of the report which correspond to the descripiton of the radar antecedent and current rainfall for the campaign date.
        seccion          = path to the png or jpeg file containing a representation of the tranversal section measured.
        alturas          = path to the png or jpeg file containing the hourly level of water registered during the campaign. 
        lluvia           = path to the png or jpeg file containing the radar rainfall plots to be analyzed.
        histograma       = path to the png or jpeg file containing the stattistics for the historic gauging campaigns.
        resultados       = path to the excel file containing the gauging campaign data and results for the station.
        fecha (optional) = the gauging campaign date can be set manuall or contained in the results file.
        numero_aforos    = number of gauging campaigns carried out.
        foot (ptional)   = path to the png or jpeg file containing the page foot of the report (Logos)
        head (ptional)   = path to the png or jpeg file containing the page header of the report
        estadisticas(opt) 
        heights          = set False to not display the hourly registered levels in the first figure of this report.
        page2            = set False to not display the second page of this report.
        table            = set False to not display the results table.
        one_page         = set True to only display the level and section figure (and its description) and the rainfall figure. 
        '''
        from reportlab.lib.pagesizes import letter
        from reportlab.platypus import SimpleDocTemplate,Paragraph, Table, TableStyle
        from IPython.display import IFrame
        from reportlab.pdfbase import pdfmetrics
        from reportlab.pdfbase.ttfonts import TTFont
        from reportlab.pdfgen import canvas
        from reportlab.lib import colors
        from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
        from reportlab.lib.enums import TA_LEFT, TA_CENTER, TA_RIGHT, TA_JUSTIFY
        from reportlab.lib.units import inch
        from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table
        import sys

        reload(sys)  # Reload does the trick!
        sys.setdefaultencoding('UTF8')


        barcode_font = r"/media/nicolas/Home/Jupyter/MarioLoco/Tools/AvenirLTStd-Book.ttf"
        pdfmetrics.registerFont(TTFont("AvenirBook", barcode_font))
        barcode_font = r"/media/nicolas/Home/Jupyter/MarioLoco/tools/avenir-next-bold.ttf"
        pdfmetrics.registerFont(TTFont("AvenirBookBold", barcode_font))

        head='/media/nicolas/Home/Jupyter/MarioLoco/tools/head.png' if head==None else head
        foot='/media/nicolas/Home/Jupyter/MarioLoco/tools/foot.png' if foot==None else foot

        print(head)
        print(foot)

        texto1=open(texto1).read().decode('utf8')
        texto2=open(texto2).read().decode('utf8')

        resultados=self.aforo
        fecha=self.aforo.fecha

        try:
            dispositivo=resultados.loc['dispositivo'].values[0]
        except:
            dispositivo='OTT MF-PRO'

        textf1 = kwargs.get('textf1','Figura 1. a) Dibujo de la sección transversal del canal. b) Caudales horarios obtenidos a partir de profundidades de la lámina de agua.')
        textf2 = 'Tabla 1. Resumen, muestra el dispositivo con el que se realizó el aforo y los parámetros hidráulicos estimados más relevantes.'
        textf3 = kwargs.get('textf3','Figura 2. a) Distribución temporal de la lluvia en la cuenca. La sombra azul invertida representa la intensidad promedio en mm/h. b) Distribución espacial de la lluvia acumulada en la cuenca en mm en un periodo de 36 horas.')
        text_color = '#%02x%02x%02x' % (8,31,45)
        widthPage =  816
        heightPage = 1056
        pdf = canvas.Canvas(nombre_archivo,pagesize=(widthPage,heightPage))
        cx = 0
        cy = 900
        #pdf.drawImage(ruteSave,20,250,width=860,height=650)
        pdf.drawImage(foot,816/2-(100/(209/906.))/2,10,width=(100/(209/906.)),height=100)
        pdf.drawImage(head,0,1056-129,width=816,height=129)
        text_color = '#%02x%02x%02x' % (8,31,45)
        styles=getSampleStyleSheet()
        styles.add(ParagraphStyle(name='Texts', alignment=TA_CENTER, fontName = "AvenirBook", fontSize = 20, textColor = text_color, leading = 20))
        styles.add(ParagraphStyle(name='Justify', alignment=TA_JUSTIFY, fontName = "AvenirBook", fontSize = 14, textColor = text_color, leading = 20))
        styles.add(ParagraphStyle(name='JustifyBold', alignment=TA_JUSTIFY, fontName = "AvenirBookBold", fontSize = 13, textColor = text_color, leading = 20))
        #flagheigths
        if heights == False:
            height = 180
            width = self.pixelconverter(seccion,height=height)
            xloc = widthPage/2.0 - (width/2.0)
            pdf.drawImage(seccion,xloc,550,width = width,height = height)
            p = Paragraph('Figura 1. Dibujo de la sección transversal del canal', styles["JustifyBold"])
            p.wrapOn(pdf, 716, 200)
            p.drawOn(pdf,270,490)

        else:
            pdf.drawImage(seccion,50,550,width=310,height=211)
            pdf.setFont("AvenirBook", 14)
            pdf.drawString(220,770,"a)")
            pdf.drawString(600,770,"b)")
            pdf.drawImage(alturas,50+310,550,width=426-13,height=211)
            p = Paragraph(textf1, styles["JustifyBold"])
            p.wrapOn(pdf, 716, 200)
            p.drawOn(pdf,50,480)

        if len(texto1)<470:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,830)

        elif len(texto1)<540:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,815)

        elif len(texto1)<580:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,800)

        elif len(texto1)<640:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,775)

        elif len(texto1)<700:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,760)

        else:
            p = Paragraph(texto1, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,750)


        pdf.setFillColor(text_color)
        pdf.setFont("AvenirBook", 20)
        print(nombreEstacion)

        p = Paragraph(u'Estación %s - %s'%(nombreEstacion.encode('utf8'),fecha), styles["Texts"])
        p.wrapOn(pdf, 816, 200)
        p.drawOn(pdf,0,945)

        data= [['Caudal total [m^3/s] ', round(float(resultados.caudal_medio),2), 'Dispositivo', dispositivo], [u'Área mojada [m^2]',round(float(resultados.area_total),2), 'Ancho superficial [m]',round(float(resultados.ancho_superficial),2)], ['Profundidad media [m]', round(float(resultados.altura_media),2), 'Velocidad promedio [m/s]',round(float(resultados.velocidad_media),2)], [u'Perímetro mojado [m]', round(float(resultados.perimetro),2), 'Radio hidráulico [m]', round(float(resultados.radio_hidraulico),2)],]

        if table==True:
            t=Table(data,colWidths = [210,110,210,110],rowHeights=[30,30,30,30],style=[('GRID',(0,0),(-1,-1),1,text_color), ('ALIGN',(0,0),(0,-1),'LEFT'),('BACKGROUND',(0,0),(0,-1),colors.white), ('ALIGN',(3,2),(3,2),'LEFT'), ('BOX',(0,0),(-1,-1),1,colors.black), ('TEXTFONT', (0, 0), (-1, 1), 'AvenirBook'), ('TEXTCOLOR',(0,0),(-1,-1),text_color), ('FONTSIZE',(0,0),(-1,-1),14), ('VALIGN',(0,0),(-1,-1),'MIDDLE'), ('ALIGN',(1,0),(1,-1),'CENTER'), ('ALIGN',(3,0),(3,-1),'CENTER') ])

            t.wrapOn(pdf, 650, 200)
            t.drawOn(pdf,100,310)

            p = Paragraph(textf2, styles["JustifyBold"])
            p.wrapOn(pdf, 716, 200)
            p.drawOn(pdf,50,240)

        pdf.setFont("AvenirBookBold", 14)
        pdf.setFillColor('#%02x%02x%02x' % (8,31,45))
        pdf.setFont("AvenirBook", 15)
        pdf.setFillColor('#%02x%02x%02x' % (8,31,45))

        if one_page==True:
            page2=False
            height = 225
            width = self.pixelconverter(lluvia,height=height)
            xloc = widthPage/2.0 - (width/2.0)
            pdf.drawImage(lluvia,xloc,230,width = width,height = height)
            pdf.drawImage('/media/nicolas/Home/Jupyter/MarioLoco/tools/acumuladoLegend.jpg',642,255,width=43.64,height=200)
            p = Paragraph(textf3, styles["JustifyBold"])
            p.wrapOn(pdf, 716, 200)
            p.drawOn(pdf,50,130)

        pdf.showPage()
        
        #PÁGINA 2 
        if page2==True:
            pdf.drawImage(foot,816/2-(100/(209/906.))/2,10,width=(100/(209/906.)),height=100)
            pdf.drawImage(head,0,1056-129,width=816,height=129)
            height = 225
            width = self.pixelconverter(lluvia,height=height)
            xloc = widthPage/2.0 - (width/2.0)
            pdf.drawImage(lluvia,xloc,540,width = width,height = height)
            p = Paragraph(u'Estación %s - %s'%(nombreEstacion.encode('utf8'),fecha), styles["Texts"])
            p.wrapOn(pdf, 816, 200)
            p.drawOn(pdf,0,945)
            
        if len(texto2)<500:
            p = Paragraph(texto2, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,820)

        else:
            p = Paragraph(texto2, styles["Justify"])
            p.wrapOn(pdf, 720, 200)
            p.drawOn(pdf,50,790)

            textf4 = 'Figura 3. a) Distribuciones de frecuencia, número de aforos: %s, la línea punteada vertical es el caudal observado, la curva es una distribución de frecuencia acumulada que presenta el régimen de caudales. b) Resumen de estadísticos. Max = Caudal máximo, Min = Caudal mínimo, P25 = Percentil 25, P50 = Mediana, P75 = Percentil 75, Media = Caudal promedio, Std = desviación estándar, Obs = Caudal observado.'%(numero_aforos)

            p = Paragraph(textf3, styles["JustifyBold"])
            p.wrapOn(pdf, 716, 200)
            p.drawOn(pdf,50,480)

            # distribuciones
            if numero_aforos>0:
                if estadisticas == False:
                    height = 230
                    width = self.pixelconverter(histograma,height=height)
                    xloc = widthPage/2.0 - (width/2.0)
                    pdf.drawImage(histograma,xloc,220,width = width,height = height)
                    p = Paragraph(textf4, styles["JustifyBold"])
                    p.wrapOn(pdf, 716, 200)
                    p.drawOn(pdf,50,125)
                    pdf.setFont("AvenirBook", 14)
                    pdf.drawString(205,460,"a)")
                    pdf.drawString(590,460,"b)")

                else:
                    textf4 = estadisticas
                    pdf.drawImage(histograma,155,180,width=500,height=250)
                    p = Paragraph(textf4, styles["JustifyBold"])
                    p.wrapOn(pdf, 716, 200)
                    p.drawOn(pdf,120,145)

                # pdf.drawImage('/media/nicolas/Home/Jupyter/MarioLoco/tools/acumuladoLegend.jpg',642,570,width=43.64,height=200)
                pdf.drawImage('/media/nicolas/Home/Jupyter/MarioLoco/tools/arrow.png', kwargs.get('left',595), 575, width=20, height=20)
                left=kwargs.get('left',590)
                pdf.drawString(left+10,596,"N")
                pdf.setFont("AvenirBook", 14)
                pdf.setFillColor('#%02x%02x%02x' % (8,31,45))
                if left>560:
                    pdf.drawString(205,770,"a)")
                    pdf.drawString(590,770,"b)")
                x = 460
            else:
                p = Paragraph(u'Estación %s - %s'%(nombreEstacion.encode('utf8'),fecha), styles["Texts"])
                p.wrapOn(pdf, 816, 200)
                p.drawOn(pdf,0,945)
        pdf.save()
 
