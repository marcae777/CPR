#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>
import datetime
from cpr.Nivel import Nivel
import cpr.information as info
import multiprocessing
import time
import warnings
warnings.filterwarnings("ignore")

def plot_nivel_3h():
    end = datetime.datetime.now()
    self = Nivel(codigo=260,SimuBasin=True,**info.LOCAL)
    start = end-datetime.timedelta(hours=3)
    df = self.level_all(start,end,local=True)
    bad = []
    for codigo in df.columns:
        self = Nivel(codigo=codigo,**info.LOCAL)
        level = df[codigo]
        try:
            topo = self.last_topo().set_index('vertical')[['x','y']]
            filepath = self.data_path+'graficas_nivel/tres_horas/%s.png'%self.info.slug
            self.plot_operacional(level/100.0,topo,'3h',filepath)
            print(filepath)
        except:
            bad.append(codigo)
            print("ERROR:%s"%codigo)
    print(datetime.datetime.now()-end)

if __name__ == '__main__':
    p = multiprocessing.Process(target=plot_nivel_3h, name="r")
    p.start()
    time.sleep(100) # wait 20 seconds to kill process
    p.terminate()
    p.join()
