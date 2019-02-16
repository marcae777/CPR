#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
#  CRP.py
#
#  Copyright 2018 MCANO <mario.cano@siata.gov.co>

# argumentos por defecto para class SqlDb servidor local
AMAZONAS = {
    'host'                      : "localhost",
    'user'                      : "sample_user",
    'passwd'                    : "s@mple_p@ss",
    'table'                     : 'estaciones',
    'dbname'                    : "cpr",
    'port'                      : 3306
            }

DEVELOPMENT = {
    'host'                      : "localhost",
    'user'                      : "sample_user",
    'passwd'                    : "s@mple_p@ss",
    'table'                     : 'estaciones',
    'dbname'                    : "cpr",
    'port'                      : 3306
            }

# argumentos por defecto para class SqlDb servidor de siata
SIATA = {
    'host'                      : "localhost",
    'user'                      :"siata_Consulta",
    'passwd'                    :"si@t@64512_C0nsult4",
    'table'                     : 'estaciones',
    'dbname'                    :"siata",
    'port'                      : 3306
            }

RADAR_PATH = '/home/mcano/dev/cprweb/src/media/101_RadarClass/'
DATA_PATH = '/home/mcano/dev/backup/cprweb/src/media/'
REFLECTIVIDAD_PATH = '/home/mcano/storage/radar/'
