import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from pyspark.sql import SparkSession
import pandas as pd
from datetime import datetime
from pyspark.sql import functions as F, Window
from pyspark.sql.types import *
import re
import argparse
import os

sys.path.insert(1,'/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

timestart = datetime.now()

vSStep = '[Paso inicial]: Obteniendo parametros de la SHELL'
try:
    # 1.-Captura de argumentos en la entrada
    ts_step = datetime.now()
    print(lne_dvs())
    print(etq_info(vSStep))
    parser = argparse.ArgumentParser()
    parser.add_argument('--vSFile', required=True, type=str,
                        help='Ruta y nombre del archivo a leer')
    parser.add_argument('--vSChema', required=True, type=str,
                        help='Esquema donde se registraran los datos')
    parser.add_argument('--vSTable', required=True, type=str,
                        help='Tabla donde se registraran los datos')
    parser.add_argument('--vSEntidad', required=True,
                        type=str, help='Nombre del proceso')
    parser.add_argument('--vIEtapa', required=True,
                        type=int, help='Etapa de la Shell')

    parametros = parser.parse_args()
    vSFile = parametros.vSFile
    vSChema = parametros.vSChema
    vSTable = parametros.vSTable
    vSEntidad = parametros.vSEntidad
    vIEtapa = parametros.vIEtapa

    print(lne_dvs())
    print(etq_info("Imprimiendo parametros..."))
    print(lne_dvs())
    print(etq_info(log_p_parametros("vSFile", str(vSFile))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("vSTable", str(vSTable))))
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vIEtapa", str(vIEtapa))))
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = '[ETAPA => {} / Paso 1]: Configuracion Spark Session'.format(vIEtapa)
try:
    ts_step = datetime.now()
    print(etq_info(vSStep))
    print(lne_dvs())
    spark = SparkSession\
        .builder\
        .config("hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport()\
        .getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
    app_id = spark._sc.applicationId

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vStp00 = '[ETAPA => {} / Paso 2]: Iniciando proceso/Cargando configuracion..'.format(
    vIEtapa)
try:
    ts_step = datetime.now()
    print(etq_info("Mostrar application_id => {}".format(str(app_id))))
    print(lne_dvs())
    print(etq_info("Inicio del proceso en PySpark..."))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStp00, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp00, str(e))))


print(lne_dvs())
vStp01 = '[ETAPA => {} / Paso 3]: Lee el archivo y cambiar nombres a las columnas..'.format(
    vIEtapa)
try:
    ts_step = datetime.now()
    
    df0 = spark.read. \
        option("header", "false"). \
        option("delimiter", ","). \
        option("encoding", "LATIN1"). \
        csv(vSFile)
    df0.printSchema()
    df0.show(3)

    df1 = df0.withColumnRenamed("_c0", "nombre_plaza"). \
        withColumnRenamed("_c1", "jefe_de_venta")
    df1.printSchema()
    print(etq_info("Numero de rows file: {}".format(str(df1.count()))))
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vStp01, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp01, str(e))))


print(lne_dvs())
vStp03 = '[ETAPA => {} / Paso 4] Drop table e insert en la tabla destino..'.format(
    vIEtapa)
try:
    ts_step = datetime.now()
    nme_table = vSChema+"."+vSTable
    print(etq_info(msg_i_insert_hive(nme_table)))
    try:
        ts_step_tbl = datetime.now()
        
        df1.write.mode("overwrite").saveAsTable(nme_table)
        df1.printSchema()

        te_step_tbl = datetime.now()
        print(etq_info(msg_d_duracion_hive(
            nme_table, vle_duracion(ts_step_tbl, te_step_tbl))))
    except Exception as e:
        exit(etq_error(msg_e_insert_hive(nme_table, str(e))))
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStp03, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStp03, str(e))))


print(lne_dvs())
vStpFin = '[ETAPA => {} / Paso Final]: Eliminando dataframes ..'.format(
    vIEtapa)
print(lne_dvs())
try:
    ts_step = datetime.now()
    del df0
    del df1
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vStpFin, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vStpFin, str(e))))


print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(
    vSEntidad, vle_duracion(timestart, timeend))))
print(lne_dvs())
