#!/usr/bin/env python
import pandas as pd
import datetime as dt
import os
from multiprocessing import Pool
import numpy as np
import pickle
import smtplib

# Texto Fecha: el texto de fecha que se usa para guardar algunos archivos de figuras.
dateText = dt.datetime.now().strftime('%Y%m%d%H%M')


#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||||
#print '###################################################### CRON DE CONVERSION DE RADAR A LLUVIA #######################################'

#-------------------------------------------------------------------
#GENERA LA FECHA ACTUAL
#-------------------------------------------------------------------
# Obtiene el datetime
fecha_1 =  dt.datetime.now() + dt.timedelta(hours = 5) - dt.timedelta(minutes = 15)
fecha_2 =  dt.datetime.now() + dt.timedelta(hours = 5) + dt.timedelta(minutes = 15)
# Lo convierte en texto
fecha1 = fecha_1.strftime('%Y-%m-%d')
fecha2 = fecha_2.strftime('%Y-%m-%d')
hora_1 = fecha_1.strftime('%H:%M')
hora_2 = fecha_2.strftime('%H:%M')

#-------------------------------------------------------------------
#RUTAS DE TRABAJO
#-------------------------------------------------------------------
rutaCodigo = '/home/mcano/dev/cpr/crones/Radar2RainConvStra.py'
rutaRadar = '/home/mcano/dev/cprweb/src/media/radar/'
rutaNC = '/home/mcano/dev/cprweb/src/media/101_RadarClass/'

#-------------------------------------------------------------------
#Campo de lluvia en los ultimos 5min
#-------------------------------------------------------------------
comando = rutaCodigo+' '+fecha1+' '+fecha2+' '+rutaRadar+' '+rutaNC+' -1 '+hora_1+' -2 '+hora_2+' -v'
os.system(comando)
print (comando)

################
#Correo - Soraya
################

#Se revisan los archivos en la ruta donde se deben estar actualizando.
now = dt.datetime.now()
ago = now-dt.timedelta(minutes=10)
ruta='/media/nicolas/Home/nicolas/101_RadarClass/'
times=[]

for root, dirs,files in os.walk(ruta):
    #se itera sobre todos los archivos en la ruta.
    for fname in files:
        # se llega a cada archivo
        path = os.path.join(root, fname)
        #se obtiene la fecha de modificacion de cada path
        mtime = dt.datetime.fromtimestamp(os.stat(path).st_mtime)
        #si hay alguno modificado en los ultimos 'ago' minutos entonces se guarda.
        if mtime > ago:
            times.append(mtime.strftime('%Y%m%d-%H:%M'))
#             print('%s modified %s'%(path, mtime))

# si no hay ningun archivo modificado en los ultimos 'ago' minutos manda un correo a Soraya
if len(times)==0:
    to='scgiraldo11@gmail.com'
    Asunto='SC_CamposRadar no se ha ejecutado'
    Mensaje='No se han generado archivos .nc de radar en Amazonas (192.168.1.12): '+ruta+' en los ultimos 20 min.'
    gmail_user = 'scgiraldo11@gmail.com'
    gmail_pwd = '12345s.oraya'
    smtpserver = smtplib.SMTP("smtp.gmail.com",587)
    smtpserver.ehlo()
    smtpserver.starttls()
    smtpserver.ehlo
    smtpserver.login(gmail_user, gmail_pwd)
    header = 'To:' + to + '\n' + 'From: ' + gmail_user + '\n' + 'Subject:'+Asunto+' \n'
    msg = header + '\n'+ Mensaje +'\n\n'
    smtpserver.sendmail(gmail_user, to , msg)
    smtpserver.close()

# si hay, crea el .prc en donde debe estar por defecto.
else:
############
####PRC#####
############
    f = open('/home/nicolas/self_code/Crones/SC_CamposRadar.prc','w') #opens file with name of "test.txt"
    #linea 1: nombre prc
    f.write('SC_CamposRadar\n')
    #linea 2: descripcion
    f.write('Generacion de archivos .nc de radar en Amazonas (192.168.1.12): en /media/nicolas/Home/nicolas/101_RadarClass/\n')
    #linea 3: cada cuanto se corre el proceso
    f.write('5\n')
    #- fecha de generacion del prc
    #linea 4: ano en 4 digitos
    f.write(dt.datetime.now().strftime('%Y')+'\n')
    #linea 5: mes en 2 digitos
    f.write(dt.datetime.now().strftime('%m')+'\n')
    #linea 6: dia en 2 digitos
    f.write(dt.datetime.now().strftime('%d')+'\n')
    #linea 7: hora en 2 digitos
    f.write(dt.datetime.now().strftime('%H')+'\n')
    #linea 8: minutos en 2 digitos
    f.write(dt.datetime.now().strftime('%M')+'\n')
    #linea 9: numero de ejecuciones limite en que el prc no se ha ejecutado
    f.write('4\n') # 20  min.
    #linea 10: lista de correos adicionales al de los operacionales al que se quiere enviar
    f.write('scgiraldo11@gmail.com nicolas.velasquezgiron@gmail.com\n')
    #linea 11: mensaje
    f.write('No se han generado archivos .nc de radar en Amazonas (192.168.1.12): /media/nicolas/Home/nicolas/101_RadarClass/ en los ultimos 20 min.')
    #se termina de crear .prc
    f.close()
    # Se copia el .prc en la ruta donde debe estar en el servidor
    #Para que esto funcione se debe hacer ssh-copy-id desde el usuario e ip donde se envia hacia el usuario  e ip donde se recibe para que el login quede automatico
    os.system('scp /home/nicolas/self_code/Crones/SC_CamposRadar.prc socastillogi@192.168.1.74:/home/torresiata/SIVP/archivosPRC/')
