#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>
import matplotlib
matplotlib.use('Agg')
import datetime
from cpr.Nivel import Nivel
import cpr.information as info
import multiprocessing
import time
import warnings
warnings.filterwarnings("ignore")

def conversion():
    '''radar raw data to rain'''
    self = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
    end = datetime.datetime.now()
    start = end-datetime.timedelta(minutes=15)
    end = end + datetime.timedelta(minutes=30)
    self.reflectividad_to_rain(start,end)

if __name__ == '__main__':
    p = multiprocessing.Process(target=conversion, name="r")
    p.start()
    time.sleep(20) # wait 20 seconds to kill process
    p.terminate()
    p.join()

def reportes():
    self = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
    end = datetime.datetime.now()
    start = end - datetime.timedelta(hours=3)
    df = self.level_all(start,end,calidad=True)
    print('generando reporte de nivel')
    try:
        self.reporte_nivel(df=df)
    except:
        print('no se genera reporte de nivel')
    print('insertando datos de siata a local')
    self.insert_myusers_hydrodata(df.resample('5min').max())
    print('generando reporte de lluvia')
    self.mean_rain_report(start,end,level=df.iloc[-3:].max())

if __name__ == '__main__':
    p = multiprocessing.Process(target=reportes, name="r")
    p.start()
    time.sleep(60) # wait 60 seconds to kill process
    p.terminate()
    p.join()
