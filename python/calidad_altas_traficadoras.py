# -*- coding: utf-8 -*-

import sys
reload(sys)
sys.setdefaultencoding('utf8')

import os
import argparse
from datetime import datetime

from pyspark.sql.functions import col, lit, desc
from pyspark.sql import SparkSession

from query import *

sys.path.insert(1, '/var/opt/tel_spark')
from messages import *
from functions import *
from create import *

timestart = datetime.now()


def sumar_trafico_datos(REPORTE, NUEVA):
    REPORTE = REPORTE.withColumn(NUEVA, (col("total_trafico_2g") + col(
        "total_trafico_3g") + col("total_trafico_lte") + col("total_trafico_otro")))
    return REPORTE.select("telefono", "fecha_alta", NUEVA)


print(lne_dvs())
vSStep = '[Paso 1]: Obteniendo parametros de la SHELL'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    parser = argparse.ArgumentParser()
    parser.add_argument('--vSEntidad', required=True, type=str, help='Entidad del proceso')
    parser.add_argument('--vSChema', required=True, type=str, help='')
    parser.add_argument('--vSChemaTmp', required=True, type=str, help='')
    parser.add_argument('--FECHA_EJECUCION', required=True, type=str, help='')
    parser.add_argument('--FECHA_INICIO', required=True, type=str, help='')
    parser.add_argument('--FECHA_ACTUAL', required=True, type=str, help='')
    parser.add_argument('--FECHA_PROCESO', required=True, type=str, help='')
    parser.add_argument('--FECHA_EJECUCION_ANTERIOR', required=True, type=str, help='')
    parser.add_argument('--FECHA_13_MESES_ATRAS', required=True, type=str, help='')
    parser.add_argument('--ELIMINAR_PARTICION_PREVIA', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_1', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_2', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_3', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_4', required=True, type=str, help='')
    parser.add_argument('--FECHA_FIN_MES_5', required=True, type=str, help='')

    parametros = parser.parse_args()
    vSEntidad = parametros.vSEntidad
    vSChema = parametros.vSChema
    vSChemaTmp = parametros.vSChemaTmp
    FECHA_EJECUCION = parametros.FECHA_EJECUCION
    FECHA_INICIO = parametros.FECHA_INICIO
    FECHA_ACTUAL = parametros.FECHA_ACTUAL
    FECHA_PROCESO = parametros.FECHA_PROCESO
    FECHA_EJECUCION_ANTERIOR = parametros.FECHA_EJECUCION_ANTERIOR
    FECHA_13_MESES_ATRAS = parametros.FECHA_13_MESES_ATRAS
    ELIMINAR_PARTICION_PREVIA = parametros.ELIMINAR_PARTICION_PREVIA
    FECHA_FIN_MES_1 = parametros.FECHA_FIN_MES_1
    FECHA_FIN_MES_2 = parametros.FECHA_FIN_MES_2
    FECHA_FIN_MES_3 = parametros.FECHA_FIN_MES_3
    FECHA_FIN_MES_4 = parametros.FECHA_FIN_MES_4
    FECHA_FIN_MES_5 = parametros.FECHA_FIN_MES_5

    print(etq_info("Imprimiendo parametros..."))
    print(etq_info(log_p_parametros("vSEntidad", str(vSEntidad))))
    print(etq_info(log_p_parametros("vSChema", str(vSChema))))
    print(etq_info(log_p_parametros("vSChemaTmp", str(vSChemaTmp))))
    print(etq_info(log_p_parametros("FECHA_EJECUCION", str(FECHA_EJECUCION))))
    print(etq_info(log_p_parametros("FECHA_INICIO", str(FECHA_INICIO))))
    print(etq_info(log_p_parametros("FECHA_ACTUAL", str(FECHA_ACTUAL))))
    print(etq_info(log_p_parametros("FECHA_PROCESO", str(FECHA_PROCESO))))
    print(etq_info(log_p_parametros("FECHA_EJECUCION_ANTERIOR", str(FECHA_EJECUCION_ANTERIOR))))
    print(etq_info(log_p_parametros("FECHA_13_MESES_ATRAS", str(FECHA_13_MESES_ATRAS))))
    print(etq_info(log_p_parametros("ELIMINAR_PARTICION_PREVIA", str(ELIMINAR_PARTICION_PREVIA))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_1", str(FECHA_FIN_MES_1))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_2", str(FECHA_FIN_MES_2))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_3", str(FECHA_FIN_MES_3))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_4", str(FECHA_FIN_MES_4))))
    print(etq_info(log_p_parametros("FECHA_FIN_MES_5", str(FECHA_FIN_MES_5))))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = '[Paso 2]: Configuracion Spark Session'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    spark = SparkSession. \
        builder. \
        config("hive.exec.dynamic.partition.mode", "nonstrict"). \
        config("hive.exec.compress.output", "true"). \
        config("spark.sql.parquet.compression.codec", "snappy"). \
        enableHiveSupport(). \
        getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    app_id = spark._sc.applicationId

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [3]: Eliminar tablas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    print(etq_info(str(vSStep)))
    print(lne_dvs())

    spark.sql(drop_tmp(vSChemaTmp, "universo_altas_traficadoras_fuente"))
    spark.sql(drop_tmp(vSChemaTmp, "universo_altas_traficadoras_fuente_final"))
    spark.sql(drop_tmp(vSChemaTmp, "catalogo_bonos_pdv"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_combos_bonos_recargas_fuente_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_combos_bonos_fuente_2_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_combos_fuente_3_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_combos_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_bonos_fuente_3_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_bonos_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_recargas_fuente_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_recargas_fuente_2_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_recargas_fuente_3_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_recargas_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_reporte_recargas_combos_bonos_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_reporte_recargas_combos_bonos_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_trafico_datos_fuente_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_trafico_voz_fuente_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_lineas_traficadoras_cat"))
    spark.sql(drop_tmp(vSChemaTmp, "otc_t_lineas_traficadoras_final_cat"))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(
        vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [4]: Generar universo altas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [4.1]: Se consulta la tabla db_cs_altas.otc_t_altas_bi para generar el universo de lineas a analizar'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_universo_altas(
        FECHA_FIN_MES_1,
        FECHA_FIN_MES_2,
        FECHA_FIN_MES_3,
        FECHA_FIN_MES_4,
        FECHA_FIN_MES_5,
        FECHA_EJECUCION
    )
    print(etq_sql(vSQL))
    universo_altas = spark.sql(vSQL)

    # Se eliminan lineas cuya fecha alta sea mayor que la fecha del proceso
    FECHA_INICIO_FORMATEADA = datetime.strptime(str(FECHA_INICIO), '%Y%m%d')
    universo_altas = universo_altas.where(col("fecha_alta") >= FECHA_INICIO_FORMATEADA)

    if universo_altas.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('universo_altas'))))
    else:
        universo_altas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.universo_altas_traficadoras_fuente".format(vSChemaTmp))
        del universo_altas

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [5]: Transformar universo altas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [5.1]: Se transforma la tabla que contienen las lineas obtenidas para generar los rangos de fecha para cada uno de los 6 meses a analizar'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_universo_altas(vSChemaTmp)
    print(etq_sql(vSQL))
    universo_altas = spark.sql(vSQL)
    
    universo_altas.groupBy("mes").count().show()

    if universo_altas.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('universo_altas'))))
    else:
        # pass
        universo_altas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.universo_altas_traficadoras_fuente_final".format(vSChemaTmp))
        
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [6]: Generar catalogo bonos PDV'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSQL = q_generar_cat_bonos_pdv()
    print(etq_sql(vSQL))
    catalogo_bonos_pdv = spark.sql(vSQL)

    if catalogo_bonos_pdv.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('catalogo_bonos_pdv'))))
    else:
        catalogo_bonos_pdv.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.catalogo_bonos_pdv".format(vSChemaTmp))
        del catalogo_bonos_pdv

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [7]: Cargar eventos combos/bonos/recargas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [7.1]: Se consulta la tabla db_cs_recargas.otc_t_cs_detalle_recargas para generar el universo de combos/bonos/recargas a analizar'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_combos_bonos_recargas_fuente(FECHA_INICIO, FECHA_ACTUAL)
    print(etq_sql(vSQL))
    combos_bonos_recargas = spark.sql(vSQL)

    if combos_bonos_recargas.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('combos_bonos_recargas'))))
    else:
        combos_bonos_recargas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_combos_bonos_recargas_fuente_cat".format(vSChemaTmp))
        del combos_bonos_recargas
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
    print(lne_dvs())

    ts_step = datetime.now()
    print(etq_info(str(vSStep)))
    print(lne_dvs())

    vSStep = 'Paso [7.2]: Se aplican filtros y uniones entre el universo de lineas y universo de combos/bonos'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_combos_bonos_fuente(vSChemaTmp, "otc_t_combos_bonos_recargas_fuente_cat")
    print(etq_sql(vSQL))
    combos_bonos = spark.sql(vSQL)

    if combos_bonos.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('combos_bonos'))))
    else:
        combos_bonos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_combos_bonos_fuente_2_cat".format(vSChemaTmp))
        del combos_bonos
    
    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [8]: Generar reporte combos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    tipo_reporte = "COMBO"

    vSStep = 'Paso [8.1]: Se aplican filtros y uniones entre el universo de lineas y universo de combos'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_combos_bonos_fuente_2(vSChemaTmp, "otc_t_combos_bonos_fuente_2_cat", tipo_reporte)
    print(etq_sql(vSQL))
    reporte_combos = spark.sql(vSQL)
    reporte_combos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_combos_fuente_3_cat".format(vSChemaTmp))

    vSQL = q_transformar_combos_bonos_fuente_3(vSChemaTmp, "otc_t_combos_fuente_3_cat")
    print(etq_sql(vSQL))
    reporte_combos = spark.sql(vSQL)
    reporte_combos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_combos_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [8.2]: Se genera un dataframe por cada uno de los 6 meses de combos y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0", 0)
    print(etq_sql(vSQL))
    reporte_combos_30 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1", 1)
    print(etq_sql(vSQL))
    reporte_combos_60 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2", 2)
    print(etq_sql(vSQL))
    reporte_combos_90 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3", 3)
    print(etq_sql(vSQL))
    reporte_combos_120 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4", 4)
    print(etq_sql(vSQL))
    reporte_combos_150 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_combos_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5", 5)
    print(etq_sql(vSQL))
    reporte_combos_180 = spark.sql(vSQL)

    reporte_combos = universo_altas.select("telefono", "fecha_alta").join(reporte_combos_30, ["telefono", "fecha_alta"], how="left")
    reporte_combos = reporte_combos.join(reporte_combos_60, ["telefono", "fecha_alta"], how="left")
    reporte_combos = reporte_combos.join(reporte_combos_90, ["telefono", "fecha_alta"], how="left")
    reporte_combos = reporte_combos.join(reporte_combos_120, ["telefono", "fecha_alta"], how="left")
    reporte_combos = reporte_combos.join(reporte_combos_150, ["telefono", "fecha_alta"], how="left")
    reporte_combos = reporte_combos.join(reporte_combos_180, ["telefono", "fecha_alta"], how="left")

    if reporte_combos.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_combos'))))
    else:
        del reporte_combos_180
        del reporte_combos_150
        del reporte_combos_120
        del reporte_combos_90
        del reporte_combos_60
        del reporte_combos_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [9]: Generar reporte bonos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    tipo_reporte = "BONO"

    vSStep = 'Paso [9.1]: Se aplican filtros y uniones entre el universo de lineas y universo de bonos'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_combos_bonos_fuente_2(vSChemaTmp, "otc_t_combos_bonos_fuente_2_cat", tipo_reporte)
    print(etq_sql(vSQL))
    reporte_bonos = spark.sql(vSQL)
    reporte_bonos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_bonos_fuente_3_cat".format(vSChemaTmp))

    vSQL = q_transformar_combos_bonos_fuente_3(vSChemaTmp, "otc_t_bonos_fuente_3_cat")
    print(etq_sql(vSQL))
    reporte_bonos = spark.sql(vSQL)
    reporte_bonos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_bonos_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [9.2]: Se genera un dataframe por cada uno de los 6 meses de bonos y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0", 0)
    print(etq_sql(vSQL))
    reporte_bonos_30 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1", 1)
    print(etq_sql(vSQL))
    reporte_bonos_60 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2", 2)
    print(etq_sql(vSQL))
    reporte_bonos_90 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3", 3)
    print(etq_sql(vSQL))
    reporte_bonos_120 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4", 4)
    print(etq_sql(vSQL))
    reporte_bonos_150 = spark.sql(vSQL)

    vSQL = q_generar_reporte_combos_bonos_resumido(tipo_reporte.lower(), vSChemaTmp, "otc_t_bonos_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5", 5)
    print(etq_sql(vSQL))
    reporte_bonos_180 = spark.sql(vSQL)

    reporte_bonos = universo_altas.select("telefono", "fecha_alta").join(reporte_bonos_30, ["telefono", "fecha_alta"], how="left")
    reporte_bonos = reporte_bonos.join(reporte_bonos_60, ["telefono", "fecha_alta"], how="left")
    reporte_bonos = reporte_bonos.join(reporte_bonos_90, ["telefono", "fecha_alta"], how="left")
    reporte_bonos = reporte_bonos.join(reporte_bonos_120, ["telefono", "fecha_alta"], how="left")
    reporte_bonos = reporte_bonos.join(reporte_bonos_150, ["telefono", "fecha_alta"], how="left")
    reporte_bonos = reporte_bonos.join(reporte_bonos_180, ["telefono", "fecha_alta"], how="left")

    if reporte_bonos.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_bonos'))))
    else:
        del reporte_bonos_180
        del reporte_bonos_150
        del reporte_bonos_120
        del reporte_bonos_90
        del reporte_bonos_60
        del reporte_bonos_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [10]: Generar reporte recargas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [10.1]: Se aplican filtros y uniones entre el universo de lineas y universo de recargas'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_recargas_fuente(vSChemaTmp, "otc_t_combos_bonos_recargas_fuente_cat")
    print(etq_sql(vSQL))
    reporte_recargas = spark.sql(vSQL)
    reporte_recargas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_recargas_fuente_2_cat".format(vSChemaTmp))

    vSQL = q_transformar_recargas_fuente_2(vSChemaTmp, "otc_t_recargas_fuente_2_cat")
    print(etq_sql(vSQL))
    reporte_recargas = spark.sql(vSQL)
    reporte_recargas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_recargas_fuente_3_cat".format(vSChemaTmp))

    vSQL = q_transformar_recargas_fuente_3(vSChemaTmp, "otc_t_recargas_fuente_3_cat")
    print(etq_sql(vSQL))
    reporte_recargas = spark.sql(vSQL)
    reporte_recargas.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_recargas_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [10.3]: Se genera un dataframe por cada uno de los 6 meses de recargas y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0", 0)
    print(etq_sql(vSQL))
    reporte_recargas_30 = spark.sql(vSQL)

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1", 1)
    print(etq_sql(vSQL))
    reporte_recargas_60 = spark.sql(vSQL)

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2", 2)
    print(etq_sql(vSQL))
    reporte_recargas_90 = spark.sql(vSQL)

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3", 3)
    print(etq_sql(vSQL))
    reporte_recargas_120 = spark.sql(vSQL)

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4", 4)
    print(etq_sql(vSQL))
    reporte_recargas_150 = spark.sql(vSQL)

    vSQL = q_generar_reporte_recargas_resumido(vSChemaTmp, "otc_t_recargas_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5", 5)
    print(etq_sql(vSQL))
    reporte_recargas_180 = spark.sql(vSQL)

    reporte_recargas = universo_altas.select("telefono", "fecha_alta").join(reporte_recargas_30, ["telefono", "fecha_alta"], how="left")
    reporte_recargas = reporte_recargas.join(reporte_recargas_60, ["telefono", "fecha_alta"], how="left")
    reporte_recargas = reporte_recargas.join(reporte_recargas_90, ["telefono", "fecha_alta"], how="left")
    reporte_recargas = reporte_recargas.join(reporte_recargas_120, ["telefono", "fecha_alta"], how="left")
    reporte_recargas = reporte_recargas.join(reporte_recargas_150, ["telefono", "fecha_alta"], how="left")
    reporte_recargas = reporte_recargas.join(reporte_recargas_180, ["telefono", "fecha_alta"], how="left")

    if reporte_recargas.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_recargas'))))
    else:
        del reporte_recargas_180
        del reporte_recargas_150
        del reporte_recargas_120
        del reporte_recargas_90
        del reporte_recargas_60
        del reporte_recargas_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [11]: Generar la tabla otc_t_reporte_recargas_combos_bonos_final_cat con informacion mensual de combos/bonos/recargas'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    reporte_recargas_combos_bonos = universo_altas.select("telefono", "fecha_alta").join(reporte_recargas, ["telefono", "fecha_alta"], how="left")
    reporte_recargas_combos_bonos = reporte_recargas_combos_bonos.join(reporte_combos, ["telefono", "fecha_alta"], how="left")
    reporte_recargas_combos_bonos = reporte_recargas_combos_bonos.join(reporte_bonos, ["telefono", "fecha_alta"], how="left")

    reporte_recargas_combos_bonos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_reporte_recargas_combos_bonos_cat".format(vSChemaTmp))

    vSQL = q_generar_reporte_recargas_combos_bonos(vSChemaTmp, "otc_t_reporte_recargas_combos_bonos_cat")
    print(etq_sql(vSQL))
    reporte_recargas_combos_bonos = spark.sql(vSQL)
    reporte_recargas_combos_bonos = reporte_recargas_combos_bonos.na.fill(value=0)

    if reporte_recargas_combos_bonos.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_recargas_combos_bonos'))))
    else:
        reporte_recargas_combos_bonos.write.mode("overwrite").saveAsTable("{}.otc_t_reporte_recargas_combos_bonos_final_cat".format(vSChemaTmp))

        del reporte_recargas
        del reporte_bonos
        del reporte_combos

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [12]: Generar reporte datos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [12.1]: Se consulta la tabla db_cmd.otc_t_dm_cur_t2 para generar el universo de trafico datos a analizar'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_trafico_datos_fuente(FECHA_INICIO, FECHA_ACTUAL)
    print(etq_sql(vSQL))
    reporte_datos = spark.sql(vSQL)
    reporte_datos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_trafico_datos_fuente_cat".format(vSChemaTmp))

    vSStep = 'Paso [12.2]: Se aplican filtros y uniones entre el universo de lineas y universo de trafico datos'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_trafico_datos_fuente(vSChemaTmp, "otc_t_trafico_datos_fuente_cat")
    print(etq_sql(vSQL))
    reporte_datos = spark.sql(vSQL)
    reporte_datos.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_trafico_datos_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [12.3]: Se genera un dataframe por cada uno de los 6 meses de trafico datos y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0")
    print(etq_sql(vSQL))
    reporte_datos_30 = spark.sql(vSQL)
    reporte_datos_30 = sumar_trafico_datos(reporte_datos_30, "trafico_datos_M0")

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1")
    print(etq_sql(vSQL))
    reporte_datos_60 = spark.sql(vSQL)
    reporte_datos_60 = sumar_trafico_datos(reporte_datos_60, "trafico_datos_M1")

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2")
    print(etq_sql(vSQL))
    reporte_datos_90 = spark.sql(vSQL)
    reporte_datos_90 = sumar_trafico_datos(reporte_datos_90, "trafico_datos_M2")

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3")
    print(etq_sql(vSQL))
    reporte_datos_120 = spark.sql(vSQL)
    reporte_datos_120 = sumar_trafico_datos(reporte_datos_120, "trafico_datos_M3")

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4")
    print(etq_sql(vSQL))
    reporte_datos_150 = spark.sql(vSQL)
    reporte_datos_150 = sumar_trafico_datos(reporte_datos_150, "trafico_datos_M4")

    vSQL = q_generar_trafico_datos_resumido(vSChemaTmp, "otc_t_trafico_datos_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5")
    print(etq_sql(vSQL))
    reporte_datos_180 = spark.sql(vSQL)
    reporte_datos_180 = sumar_trafico_datos(reporte_datos_180, "trafico_datos_M5")

    reporte_datos = universo_altas.select("telefono", "fecha_alta").join(reporte_datos_30, ["telefono", "fecha_alta"], how="left")
    reporte_datos = reporte_datos.join(reporte_datos_60, ["telefono", "fecha_alta"], how="left")
    reporte_datos = reporte_datos.join(reporte_datos_90, ["telefono", "fecha_alta"], how="left")
    reporte_datos = reporte_datos.join(reporte_datos_120, ["telefono", "fecha_alta"], how="left")
    reporte_datos = reporte_datos.join(reporte_datos_150, ["telefono", "fecha_alta"], how="left")
    reporte_datos = reporte_datos.join(reporte_datos_180, ["telefono", "fecha_alta"], how="left")

    if reporte_datos.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_datos'))))
    else:
        del reporte_datos_180
        del reporte_datos_150
        del reporte_datos_120
        del reporte_datos_90
        del reporte_datos_60
        del reporte_datos_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [13]: Cargar trafico voz entrante/saliente'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSStep = 'Paso [13.1]: Se consulta la tabla db_trafica.otc_t_cur_voz_trafica para generar el universo de trafico voz a analizar'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_trafico_voz_fuente(FECHA_INICIO, FECHA_ACTUAL)
    print(etq_sql(vSQL))
    trafico_voz = spark.sql(vSQL)

    if trafico_voz.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('trafico_voz'))))
    else:
        trafico_voz.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_trafico_voz_fuente_cat".format(vSChemaTmp))

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [14]: Generar reporte voz saliente'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    sentido = "SALIENTE"

    vSStep = 'Paso [14.1]: Se aplican filtros y uniones entre el universo de lineas y universo de trafico voz saliente'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_trafico_voz_fuente(vSChemaTmp, "otc_t_trafico_voz_fuente_cat", "a_direction_number")
    print(etq_sql(vSQL))
    reporte_voz_saliente = spark.sql(vSQL)
    reporte_voz_saliente.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_trafico_voz_saliente_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [14.2]: Se genera un dataframe por cada uno de los 6 meses de trafico voz saliente y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0", sentido.lower(), 0)
    print(etq_sql(vSQL))
    reporte_voz_saliente_30 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1", sentido.lower(), 1)
    print(etq_sql(vSQL))
    reporte_voz_saliente_60 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2", sentido.lower(), 2)
    print(etq_sql(vSQL))
    reporte_voz_saliente_90 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3", sentido.lower(), 3)
    print(etq_sql(vSQL))
    reporte_voz_saliente_120 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4", sentido.lower(), 4)
    print(etq_sql(vSQL))
    reporte_voz_saliente_150 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_saliente_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5", sentido.lower(), 5)
    print(etq_sql(vSQL))
    reporte_voz_saliente_180 = spark.sql(vSQL)

    reporte_voz_saliente = universo_altas.select("telefono", "fecha_alta").join(reporte_voz_saliente_30, ["telefono", "fecha_alta"], how="left")
    reporte_voz_saliente = reporte_voz_saliente.join(reporte_voz_saliente_60, ["telefono", "fecha_alta"], how="left")
    reporte_voz_saliente = reporte_voz_saliente.join(reporte_voz_saliente_90, ["telefono", "fecha_alta"], how="left")
    reporte_voz_saliente = reporte_voz_saliente.join(reporte_voz_saliente_120, ["telefono", "fecha_alta"], how="left")
    reporte_voz_saliente = reporte_voz_saliente.join(reporte_voz_saliente_150, ["telefono", "fecha_alta"], how="left")
    reporte_voz_saliente = reporte_voz_saliente.join(reporte_voz_saliente_180, ["telefono", "fecha_alta"], how="left")

    if reporte_voz_saliente.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_voz_saliente'))))
    else:
        del reporte_voz_saliente_180
        del reporte_voz_saliente_150
        del reporte_voz_saliente_120
        del reporte_voz_saliente_90
        del reporte_voz_saliente_60
        del reporte_voz_saliente_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [15]: Generar reporte voz entrante'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()
    sentido = "ENTRANTE"

    vSStep = 'Paso [15.1]: Se aplican filtros y uniones entre el universo de lineas y universo de trafico voz entrante'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_transformar_trafico_voz_fuente(vSChemaTmp, "otc_t_trafico_voz_fuente_cat", "b_direction_number")
    print(etq_sql(vSQL))
    reporte_voz_entrante = spark.sql(vSQL)
    reporte_voz_entrante.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_trafico_voz_entrante_fuente_final_cat".format(vSChemaTmp))

    vSStep = 'Paso [15.2]: Se genera un dataframe por cada uno de los 6 meses de trafico voz entrante y se los une para generar un dataframe con los 6 meses'
    print(etq_info(vSStep))
    print(lne_dvs())

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im0", "fecha_alta_fm0", sentido.lower(), 0)
    print(etq_sql(vSQL))
    reporte_voz_entrante_30 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im1", "fecha_alta_fm1", sentido.lower(), 1)
    print(etq_sql(vSQL))
    reporte_voz_entrante_60 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im2", "fecha_alta_fm2", sentido.lower(), 2)
    print(etq_sql(vSQL))
    reporte_voz_entrante_90 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im3", "fecha_alta_fm3", sentido.lower(), 3)
    print(etq_sql(vSQL))
    reporte_voz_entrante_120 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im4", "fecha_alta_fm4", sentido.lower(), 4)
    print(etq_sql(vSQL))
    reporte_voz_entrante_150 = spark.sql(vSQL)

    vSQL = q_generar_trafico_voz_resumido(vSChemaTmp, "otc_t_trafico_voz_entrante_fuente_final_cat", "fecha_alta_im5", "fecha_alta_fm5", sentido.lower(), 5)
    print(etq_sql(vSQL))
    reporte_voz_entrante_180 = spark.sql(vSQL)

    reporte_voz_entrante = universo_altas.select("telefono", "fecha_alta").join(reporte_voz_entrante_30, ["telefono", "fecha_alta"], how="left")
    reporte_voz_entrante = reporte_voz_entrante.join(reporte_voz_entrante_60, ["telefono", "fecha_alta"], how="left")
    reporte_voz_entrante = reporte_voz_entrante.join(reporte_voz_entrante_90, ["telefono", "fecha_alta"], how="left")
    reporte_voz_entrante = reporte_voz_entrante.join(reporte_voz_entrante_120, ["telefono", "fecha_alta"], how="left")
    reporte_voz_entrante = reporte_voz_entrante.join(reporte_voz_entrante_150, ["telefono", "fecha_alta"], how="left")
    reporte_voz_entrante = reporte_voz_entrante.join(reporte_voz_entrante_180, ["telefono", "fecha_alta"], how="left")

    if reporte_voz_entrante.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('reporte_voz_entrante'))))
    else:
        del reporte_voz_entrante_180
        del reporte_voz_entrante_150
        del reporte_voz_entrante_120
        del reporte_voz_entrante_90
        del reporte_voz_entrante_60
        del reporte_voz_entrante_30

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [16]: Identificar lineas traficadoras usando la siguiente regla datos > 5MB y voz > 60 segundos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    lineas_traficadoras = universo_altas.select("telefono", "fecha_alta").join(reporte_datos, ["telefono", "fecha_alta"], how="left")
    lineas_traficadoras = lineas_traficadoras.join(reporte_voz_entrante, ["telefono", "fecha_alta"], how="left")
    lineas_traficadoras = lineas_traficadoras.join(reporte_voz_saliente, ["telefono", "fecha_alta"], how="left")

    lineas_traficadoras = lineas_traficadoras.na.fill(value=0)
    lineas_traficadoras.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_lineas_traficadoras_cat".format(vSChemaTmp))

    vSQL = identificar_lineas_traficadoras(vSChemaTmp, "otc_t_lineas_traficadoras_cat")
    print(etq_sql(vSQL))
    lineas_traficadoras = spark.sql(vSQL)

    if lineas_traficadoras.rdd.isEmpty():
        exit(etq_nodata(msg_e_df_nodata(str('lineas_traficadoras'))))
    else:
        lineas_traficadoras.repartition(1).write.format("parquet").mode("overwrite").saveAsTable("{}.otc_t_lineas_traficadoras_final_cat".format(vSChemaTmp))

        del reporte_datos
        del reporte_voz_entrante
        del reporte_voz_saliente

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [17]: Eliminar reportes previos'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    if ELIMINAR_PARTICION_PREVIA == "SI":
        try:
            spark.sql(drop_partition(vSChema, "otc_t_calidad_altas_traficadoras", "fecha_proceso", FECHA_EJECUCION_ANTERIOR))
        except:
            print("Particion no encontrada")
            pass
        try:
            spark.sql(drop_partition(vSChema, "otc_t_calidad_altas_traficadoras", "fecha_proceso", FECHA_13_MESES_ATRAS))
        except:
            print("Particion no encontrada")
            pass

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
vSStep = 'Paso [18]: Generar reporte calidad altas traficadoras'
print(etq_info(vSStep))
print(lne_dvs())
try:
    ts_step = datetime.now()

    vSQL = unir_universo_altas_con_lineas_traficadoras_y_trafico(vSChema, vSChemaTmp, FECHA_EJECUCION)
    print(etq_sql(vSQL))
    spark.sql(vSQL)

    te_step = datetime.now()
    print(etq_info(msg_d_duracion_ejecucion(vSStep, vle_duracion(ts_step, te_step))))
except Exception as e:
    exit(etq_error(msg_e_ejecucion(vSStep, str(e))))


print(lne_dvs())
spark.stop()
timeend = datetime.now()
print(etq_info(msg_d_duracion_ejecucion(vSEntidad, vle_duracion(timestart, timeend))))
print(lne_dvs())
