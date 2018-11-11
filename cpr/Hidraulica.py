from django.db.models import Q
from django_pandas.io import read_frame
import os, sys
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

class Hidraulica:
    def __init__(self,item = None,workspace = 'media',**kwargs):
        self.workspace     = workspace
        self.item          = item
        self.section       = pd.DataFrame()
        self.topo          = pd.DataFrame()
        self.estacion      = None
        self.infost        = pd.DataFrame()
        self.info          = pd.Series()
        self.alturas       = pd.DataFrame(index=['06:00','07:00','08:00','09:00','10:00','11:00',
                                                 '12:00','13:00','14:00','15:00','16:00','17:00','18:00'],
                                          columns = ['profundidad','offset','lamina','caudal'])
        self.items         = pd.DataFrame()
        self.topos         = pd.DataFrame()
        self.sections      = pd.DataFrame()

    def set_from_django(self,Item,Section,Topo,estaciones,item):
        if type(item) == int:
            item = Item.objects.get(id=item)
        self.item     = item
        self.section  = read_frame(Section.objects.filter(fk = self.item.pk),verbose=False).sort_values('vertical')
        self.topo     = read_frame(Topo.objects.filter(fk = self.item.pk),verbose=False).sort_values('vertical')
        self.estacion = estaciones.objects.get(id = self.item.item_fk_id)
        self.infost   = read_frame(estaciones.objects.filter(Q(clase='Nivel')| Q(clase='Section')),verbose=False).set_index('id')
        self.info     = self.infost.loc[self.item.item_fk_id]
        self.items    = read_frame(Item.objects.filter(item_fk_id = self.estacion.id),verbose=False).id.values
        self.topos    = read_frame(Topo.objects.all(),verbose=False)
        self.sections = read_frame(Section.objects.all(),verbose=False)

    def plot_section(self,*args,**kwargs):
        if self.topo.empty:
            print("Warning: Empty DataFrame, no data to plot")
        else:
            level = kwargs.get('level',None)
            xLabel = kwargs.get('xLabel','x [m]')
            yLabel = kwargs.get('yLabel','Profundidad [m]')
            waterColor = kwargs.get('waterColor','#e5efff')
            groundColor = kwargs.get('groundColor','tan')
            fontsize= kwargs.get('fontsize',14)
            figsize = kwargs.get('figsize',(6,2))
            riskLevels = kwargs.get('riskLevels',None)
            xSensor = kwargs.get('xSensor',None)
            offset = kwargs.get('offset',0)
            scatterSize = kwargs.get('scatterSize',0.0)
            ax = kwargs.get('ax',None)
            df = self.topo.copy()
            # main plot
            if ax is None:
                fig = plt.figure(figsize=figsize)
                ax = fig.add_subplot(111)
            ax.plot(df['x'].values,df['y'].values,color='k',lw=0.5)
            ax.fill_between(np.array(df['x'].values,float),np.array(df['y'].values,float),float(df['y'].min()),color=groundColor,alpha=1.0)
            # waterLevel
            sections = []
            if level is not None:
                for data in self.get_sections(df.copy(),level):
                    #ax.hlines(level,data['x'][0],data['x'][-1],color='k',linewidth=0.5)
                    ax.fill_between(data['x'],level,data['y'],color=waterColor,alpha=0.9)
                    ax.plot(data['x'],[level]*data['x'].size,linestyle='--',alpha=0.3)
                    sections.append(data)
            # Sensor
            if (offset is not None) and (xSensor is not None):
                ax.scatter(xSensor,level,marker='v',color='k',s=30+scatterSize,zorder=22)
                ax.scatter(xSensor,level,color='white',s=120+scatterSize+10,edgecolors='k')
                #ax.annotate('nivel actual',xy=(label,level*1.2),fontsize=8)
                #ax.vlines(xSensor, level,offset,linestyles='--',alpha=0.5,color=self.colores_siata[-1])
            #labels
            ax.set_xlabel(xLabel)
            ax.set_facecolor('white')
            #risks
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
            for j in ['top','right','left']:
                ax.spines[j].set_edgecolor('white')
            ax.set_ylabel('y [m]')

    def plot_aforo(self):
        self.section['y'] = self.section['y'].abs()*(-1.0)
        x = list(self.section['x'].values)*4
        y = list(self.section['y'].values*(1-0.2))+list(self.section['y'].values*(1-0.4))+list(self.section['y'].values*(1-0.8))+self.section.y.size*[0.0]
        z = list(self.section['v02'].values)+list(self.section['v04'].values)+list(self.section['v08'].values)+list(self.section['vsup'].values)
        x+=list(self.section['x'].values)
        y+=list(self.section['y'].values)
        z+=self.section.index.size*[0]

        fig = plt.figure(figsize=(7,3))
        ax = fig.add_subplot(111)
        cm = plt.cm.get_cmap('jet')
        sc = plt.scatter(x,y,c=z,vmin=0.0,vmax=3.0,cmap=cm,s=80,zorder=20)
        cb = plt.colorbar(sc, pad=0.05)
        cb.ax.set_title('V(m/s)')
        ax.plot(self.section['x'].values,[0]*self.section.index.size,linestyle='--',alpha=0.3)
        ax.fill_between(np.array(self.section['x'].values,float),np.array(self.section['y'].values,float),float(self.section['y'].min()),color='tan',alpha=1.0)
        ax.fill_between(np.array(self.section['x'].values,float),np.array(self.section['y'].values,float),0,color='#e5efff')
        for j in ['top','right','left']:
            ax.spines[j].set_edgecolor('white')
        ax.set_ylabel('y [m]')
        ax.set_xlabel('x [m]')

    def procesa_imagen(self,images,filepath):
        image = images()
        image.document =filepath
        image.user_id = 1
        image.company = self.item
        image.save()
        print('imagen prcesada en :%s'%filepath)
        return image

    def plot_history(self):
        fig = plt.figure()
        ax = fig.add_subplot(111)
        for item in self.items:
            #section = read_frame(Section.objects.filter(fk_id = item)).sort_values('vertical')
            #print(item)
            try:
                section = self.sections.set_index('fk').loc[item].sort_values('vertical')
            except:
                section = pd.DataFrame()

            if not section.empty:
                if item==self.item.id:
                    color = 'red'
                    alpha = 1
                else:
                    color = 'blue'
                    alpha = 0.3
                try:
                    ax.plot(section['x'].values,section['y'].values,color=color,alpha=alpha)
                except:
                        print('Warning: can not plot %s'%item)
