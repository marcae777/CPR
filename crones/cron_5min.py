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

def graficas_nivel():
    end = datetime.datetime.now()
    self = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
    start = end-datetime.timedelta(days=30)
    data = self.level_all(start,end,local=True)
    bad = []
    dfs = [data[end-datetime.timedelta(hours=24):],data[end-datetime.timedelta(days=3):],data]
    for df,window,folder in zip(dfs,["24h","72h","30d"],["diario","tres_dias",'treinta_dias']):
        for codigo in df.columns:
            self = Nivel(codigo=codigo,**info.LOCAL)
            level = df[codigo]
            try:
                topo = self.last_topo().set_index('vertical')[['x','y']]
                filepath = self.data_path+'graficas_nivel/%s/%s.png'%(folder,self.info.slug)
                self.plot_operacional(level/100.0,topo,window,filepath)
                print(filepath)
            except:
                bad.append(codigo)
                print("ERROR:%s"%codigo)
        print(datetime.datetime.now()-end)

if __name__ == '__main__':
    p = multiprocessing.Process(target=graficas_nivel, name="r")
    p.start()
    time.sleep(250) # wait 20 seconds to kill process
    p.terminate()
    p.join()
