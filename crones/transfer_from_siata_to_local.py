#!/usr/bin/env python
# -*- coding: utf-8 -*-
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>
from cpr_v1.SqlDb import SiataDb
import cpr_v1.settings as info
from cpr_v1.Level import Level
import datetime
self = Level(codigo=93,**info.LOCAL)
df = self.level_all()
self.end = datetime.datetime.now()
self.start = self.end - datetime.timedelta(minutes=15)

if __name__ == '__main__':
    self.insert_data_from_df(self.level_all().resample('5min').max())
