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
    'passwd'                    : "root",
    'table'                     : 'meta_estaciones',
    'dbname'                    : "hidrologia",
    'port'                      : 3306
            }
REMOTE = {
    'host'                      : "localhost",
    'user'                      : "root",
    'passwd'                    : "root",
    'table'                     : 'estaciones',
    'dbname'                    : "siata",
    'port'                      : 3306
            }

DATA_PATH = ''
