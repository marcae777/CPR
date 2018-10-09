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

def rain_report():
    self = Nivel(codigo=99,SimuBasin=True,**info.LOCAL)
    end = datetime.datetime.now()
    start = end-datetime.timedelta(hours=3)
    df = self.level_all(start,end,calidad=True)
    risks = self.risk_df(df)
    in_risk = risks[risks.sum(axis=1)>1.0].sort_index(ascending=False).index
    for codigo in in_risk:
        self = Nivel(codigo=codigo,SimuBasin=True,**info.LOCAL)
        self.rain_report(datetime.datetime.now())

    for codigo in in_risk:
        self = Nivel(codigo=codigo,SimuBasin=True,**info.LOCAL)
        self.gif(start,end)

if __name__ == '__main__':
    p = multiprocessing.Process(target=rain_report, name="r")
    p.start()
    time.sleep(250) # wait 20 seconds to kill process
    p.terminate()
    p.join()
