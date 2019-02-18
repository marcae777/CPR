#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  CRP.py
#
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>

# argumentos por defecto para class SqlDb servidor local
import os
# READ ONLY
LOCAL = {
    'host'                      : "localhost",
    'user'                      : "root",
    'passwd'                    : "mcanoYw2E#",
    'table'                     : 'meta_estaciones',
    'dbname'                    : "hidrologia",
    'port'                      : 3306
            }
REMOTE = {
    'host'                      : "localhost",
    'user'                      : "root",
    'passwd'                    : "mcanoYw2E#",
    'table'                     : 'estaciones',
    'dbname'                    : "siata",
    'port'                      : 3306
            }

RADAR_PATH = '/home/mcano/dev/cprweb/src/media/101_RadarClass/'
DATA_PATH = '/home/mcano/dev/backup/cprweb/src/media/'
REFLECTIVIDAD_PATH = '/home/mcano/storage/radar/'
