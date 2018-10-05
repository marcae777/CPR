#!/usr/bin/env python

import argparse
import textwrap
from radar import radar
import numpy as np
import pylab as pl
import netCDF4
import datetime as dt
import os
import pandas as pd
import glob

#Parametros de entrada del trazador
parser=argparse.ArgumentParser(
	prog='RadarStraConv2Basin',
	formatter_class=argparse.RawDescriptionHelpFormatter,
	description=textwrap.dedent('''\
	Convierte campos de reflectividad de radar en lluvia y clasifica
	si es estratiforme o convectivo, finalmente guarda las matrices obtenidas
	en un archivo .nc con la informacion recopilada.
        '''))
#Parametros obligatorios
parser.add_argument("fechaI",help="(YYYY-MM-DD) Fecha de inicio de imagenes")
parser.add_argument("fechaF",help="(YYYY-MM-DD) Fecha de fin de imagenes")
parser.add_argument("rutaRadar", help = "Ruta donde se encuentran los binarios de reflectividad del radar")
parser.add_argument("rutaNC", help = "Ruta donde se van a guardar los archivos nc")
parser.add_argument("-v","--verbose",help="Informa sobre la fecha que esta agregando",
	action = 'store_true')
parser.add_argument("-1","--hora_1",help="Hora inicial de lectura de los archivos",default= None )
parser.add_argument("-2","--hora_2",help="Hora final de lectura de los archivos",default= None )
parser.add_argument("-a","--a_yuter",help="Parametro a de la metodologia de conversion de Yuter",default= 15,
	type = float)
parser.add_argument("-b","--b_yuter",help="Parametro b de la metodologia de conversion de Yuter",default= 30,
	type = float)
parser.add_argument("-m","--metodo",help="metodologia para clasificar convectivas y estratiformes (yuter, siriluk, steiner)",default= 'yuter')
parser.add_argument("-e","--extrapol",help="indica si es un barrido extrapolado o no",action = 'store_true')

#lee todos los argumentos
args=parser.parse_args()
#-------------------------------------------------------------------------------------------------------------------------------------
#OBTIENE FECHAS Y DEJA ESE TEMA LISTO
#-------------------------------------------------------------------------------------------------------------------------------------
#Corta a la hora de inicio y fin especificada
if args.hora_1 != None:
	hora1 = args.hora_1
else:
	hora1 = '00:00'
if args.hora_2 != None:
	hora2 = args.hora_2
else:
	hora2 = '00:00'
fmin = dt.datetime.strptime(args.fechaI+hora1,'%Y-%m-%d%H:%M')
fmax = dt.datetime.strptime(args.fechaF+hora2,'%Y-%m-%d%H:%M')

#Si tiene que revisar en las carpetas de radar lo hace
if args.extrapol == False:
	datesDias = pd.date_range(args.fechaI,args.fechaF,freq='D')
	a = pd.Series(np.zeros(len(datesDias)),index=datesDias)
	a = a.resample('A').sum()
	Anos = [i.strftime('%Y') for i in a.index.to_pydatetime()]

	datesDias = [d.strftime('%Y%m%d') for d in datesDias.to_pydatetime()]

	ListDays = []
	ListRutas = []
	for d in datesDias:
			try:
				L = os.listdir(args.rutaRadar+d)
				L = [l for l in L if any(map(l.startswith,Anos)) and l.endswith('010_120.bin')]
				ListDays.extend(L)
				Ruta = [args.rutaRadar+d+'/'+l for l in L
						if any(map(l.startswith,Anos)) and l.endswith('010_120.bin')]
				ListRutas.extend(Ruta)
			except:
				pass
	ListDays.sort()
	ListRutas.sort()
	ListRutasFin = []
	# Obtiene la ruta final teniendo en cuenta la ruta del dia en las carpetas de radar
	for j,i in zip(ListRutas, ListDays):
		d = dt.datetime.strptime(i[:-12],'%Y%m%d%H%M')
		if d >= fmin and d<= fmax:
			ListRutasFin.append(j)
#Si es extrapolacion no tiene que revisar en las carpetas de radar
else:
	ListRutasFin = os.listdir(args.rutaRadar)
	ListRutasFin.sort()
	ListRutasFin = [args.rutaRadar + i for i in ListRutasFin]

#-------------------------------------------------------------------------------------------------------------------------------------
#CONVERSION, CLASIFICACION Y GUARDADO
#-------------------------------------------------------------------------------------------------------------------------------------
#Carga la cuenca del AMVA

Niter = len(ListRutasFin)

Conv = np.zeros(Niter)
Stra = np.zeros(Niter)
Total = np.zeros(Niter)
rad = radar.radar_process()

for c,l in enumerate(ListRutasFin):
	Total[c] = 0
	Conv[c] = 0
	Stra[c] = 0
		#Lectura y clasificacion
	rad.read_bin(l)
	rad.Class2ConvStratiform(metodo=args.metodo, b_yuter = args.b_yuter, a_yuter = args.a_yuter)
	rad.DBZ2Rain()
	#Obtiene la ref media
	Conv[c] = rad.Z[rad.ConvStra == 2].sum() / 1e6
	Stra[c] = rad.Z[rad.ConvStra == 1].sum() / 1e6
	#guarda
	if args.extrapol == False:
		rad.save_rain_class(args.rutaNC+l[-24:-3]+'nc')
	else:
		rad.save_rain_class(args.rutaNC+l[-24:-4]+'_extrapol.nc')
	Total[c] = Conv[c] + Stra[c]
	if args.verbose:
		print('no verbose')
		#print 'avance: %.2f \n' % (float(c)/float(Niter))*100
		#print("Fecha: %s \t Total: %.3f \t Conv: %.3f \t Stra: %0.3f "% (l[-24:-12], Total[c], Conv[c], Stra[c]))
