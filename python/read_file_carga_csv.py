# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql.functions import expr
from pyspark.sql.functions import regexp_replace
import os
import argparse
import re
from pyspark.sql import functions as F, Window
from datetime import datetime
import pandas as pd
from pyspark.sql import SparkSession

sys.path.append('/var/opt/tel_spark')
from create import *
from functions import *
from messages import *

timestart = datetime.now()

parser = argparse.ArgumentParser()
parser.add_argument('--ruta', required=True, type=str,
                    help='Ruta y nombre del archivo a leer')
parser.add_argument('--file', required=True, type=str,
                    help='Ruta y nombre del archivo a leer')
parser.add_argument('--sep', required=False, type=str,
                    help='Separador en caso de ser un csv')

parametros = parser.parse_args()
vRuta = parametros.ruta
vFile = parametros.file
vSeparador = parametros.sep

vRegExpUnnamed = r"unnamed*"
vApp = "Transaformacion de archivos a CSV para lectura HDFS"

spark = SparkSession\
    .builder\
    .appName(vApp)\
    .master("local")\
    .enableHiveSupport()\
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("ERROR")
app_id = spark._sc.applicationId


print(lne_dvs())
vStp00 = 'Paso [0]: Iniciando proceso/Cargando configuracion..'
try:
    ts_step = datetime.now()
    print(etq_info("INFO: Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vApp", str(vApp))))
    print(etq_info(log_p_parametros("vRegExpUnnamed", str(vRegExpUnnamed))))
    print(etq_info(log_p_parametros("vRuta", str(vRuta))))
    print(etq_info(log_p_parametros("vSeparador", str(vSeparador))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStp00, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00, str(e))))
print(lne_dvs())


def getColumnName(vColumn=str):
    a = vColumn.lower()
    x = a.replace(' ', '_')
    y = x.replace(':', '')
    return y


print(lne_dvs())
vStp01 = 'Paso [1]: Se extrae la extension del archivo..'
try:
    ts_step = datetime.now()

    # Se extrae la extension del archivo
    ext = vFile.split(".")[1]
    file = vFile.split(".")[0]
    print(file)

    if " " in file:
        filesin = file.split(" ")[1]
        print(filesin)
        filesdown = file.split(" ")[0]
        print(filesdown)
    else:
        filesin = ''
        filesdown = file
    
    if ext == "csv":
        dfFile = pd.read_csv(vFile, sep=vSeparador)
    else:
        dfFile = pd.read_excel(vRuta+"/"+vFile, error_bad_lines=False, dtype=str)

    dfFile.to_csv(vRuta+"/"+filesdown+filesin+'.csv', sep=',', index=False, header=False, encoding="Latin-1")

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStp01, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01, str(e))))


print(lne_dvs())
vStpFin = 'Paso [Final]: Eliminando dataframes ..'
print(lne_dvs())
try:
    ts_step = datetime.now()
    del dfFile
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStpFin, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin, str(e))))
print('OK - PROCESO PYSPARK TERMINADO')


print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(
    'read_file_carga_hive.py', vle_duracion(timestart, timeend))))
print(lne_dvs())
