
def q_generar_universo_altas(FECHA_FIN_MES_1, FECHA_FIN_MES_2, FECHA_FIN_MES_3, FECHA_FIN_MES_4, FECHA_FIN_MES_5, FECHA_FIN):
    qry = """
    SELECT CASE
            WHEN portabilidad = 'SI' THEN 'Portabilidad'
            ELSE 'Alta'
        END as movimiento,
        telefono,
        fecha_alta,
        linea_negocio,
        marca,
        distribuidor,
        cod_da,
        nom_plaza,
        canal_comercial,
        sub_canal,
        provincia_ivr,
        provincia,
        ciudad,
        operadora_origen,
        icc
    FROM db_cs_altas.otc_t_altas_bi
    WHERE (
        p_fecha_proceso = {FECHA_FIN_MES_1}
        OR p_fecha_proceso = {FECHA_FIN_MES_2}
        OR p_fecha_proceso = {FECHA_FIN_MES_3}
        OR p_fecha_proceso = {FECHA_FIN_MES_4}
        OR p_fecha_proceso = {FECHA_FIN_MES_5}
        OR p_fecha_proceso = {FECHA_FIN}
    )
        AND linea_negocio = 'PREPAGO'
    """.format(
        FECHA_FIN_MES_1=FECHA_FIN_MES_1,
        FECHA_FIN_MES_2=FECHA_FIN_MES_2,
        FECHA_FIN_MES_3=FECHA_FIN_MES_3,
        FECHA_FIN_MES_4=FECHA_FIN_MES_4,
        FECHA_FIN_MES_5=FECHA_FIN_MES_5,
        FECHA_FIN=FECHA_FIN
    )
    return qry


def q_transformar_universo_altas(vSChema):
    qry = """
    SELECT a.movimiento,
        a.telefono,
        date_format(a.fecha_alta, 'dd/MM/yyyy') AS fecha_alta,
        MONTH(a.fecha_alta) AS mes,
        YEAR(a.fecha_alta) AS anio,
        a.linea_negocio,
        a.marca,
        regexp_replace(a.distribuidor, '[^a-zA-Z0-9 ]', '') AS distribuidor,
        a.cod_da AS codigo_DA,
        a.nom_plaza AS nombre_plaza,
        a.canal_comercial,
        a.sub_canal,
        a.provincia_ivr,
        a.provincia AS provincia_trafico,
        a.ciudad,
        a.operadora_origen,
        date_format(a.fecha_alta, 'yyyyMMdd') fecha_alta_im0,
        date_format(date_add(a.fecha_alta, 29),'yyyyMMdd') fecha_alta_fm0,
        date_format(date_add(a.fecha_alta, 30),'yyyyMMdd') fecha_alta_im1,
        date_format(date_add(a.fecha_alta, 59),'yyyyMMdd') fecha_alta_fm1,
        date_format(date_add(a.fecha_alta, 60),'yyyyMMdd') fecha_alta_im2,
        date_format(date_add(a.fecha_alta, 89),'yyyyMMdd') fecha_alta_fm2,
        date_format(date_add(a.fecha_alta, 90),'yyyyMMdd') fecha_alta_im3,
        date_format(date_add(a.fecha_alta, 119),'yyyyMMdd') fecha_alta_fm3,
        date_format(date_add(a.fecha_alta, 120),'yyyyMMdd') fecha_alta_im4,
        date_format(date_add(a.fecha_alta, 149),'yyyyMMdd') fecha_alta_fm4,
        date_format(date_add(a.fecha_alta, 150),'yyyyMMdd') fecha_alta_im5,
        date_format(date_add(a.fecha_alta, 179),'yyyyMMdd') fecha_alta_fm5,
        j.jefe_de_venta,
        v.vendedor AS usuario
    FROM {vSChema}.universo_altas_traficadoras_fuente a
        LEFT JOIN {vSChema}.otc_t_jefe_comercial j ON a.nom_plaza = j.nombre_plaza
        LEFT JOIN {vSChema}.otc_t_vendedores v ON a.icc = v.icc 
    """.format(vSChema=vSChema)
    return qry


def q_generar_cat_bonos_pdv():
    qry = """
    SELECT DISTINCT tipo,
        codigo_pm,
        nombre_combo_final
    FROM db_reportes.cat_bonos_pdv
    """
    return qry


def q_generar_combos_bonos_recargas_fuente(FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT fecha_proceso,
        numero_telefono AS telefono,
        valor_recarga_base,
        codigo_paquete,
        plataforma,
        rec_pkt,
        operadora,
        tipo_transaccion,
        estado_recarga,
        pais_destino,
        usuario_responsablecd,
        origen_recarga_aa
    FROM db_cs_recargas.otc_t_cs_detalle_recargas
    WHERE fecha_proceso >= {FECHA_INICIO} 
        AND fecha_proceso <= {FECHA_FIN}
    """.format(FECHA_INICIO=FECHA_INICIO, FECHA_FIN=FECHA_FIN)
    return qry


def q_transformar_combos_bonos_fuente(vSChema, TABLA):
    qry = """
    SELECT fecha_proceso,
        telefono,
        valor_recarga_base,
        codigo_paquete
    FROM {vSChema}.{TABLA}
    WHERE codigo_paquete <> '' 
        AND codigo_paquete IS NOT NULL
        AND plataforma IN ('PM')
        AND rec_pkt = 'PKT'
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_transformar_combos_bonos_fuente_2(vSChema, TABLA, TIPO_REPORTE):
    qry = """
    SELECT r.fecha_proceso,
        r.telefono,
        r.valor_recarga_base
    FROM {vSChema}.{TABLA} r
    INNER JOIN {vSChema}.catalogo_bonos_pdv b 
        ON b.codigo_pm = r.codigo_paquete
    WHERE b.tipo = '{TIPO_REPORTE}'
    """.format(vSChema=vSChema, TABLA=TABLA, TIPO_REPORTE=TIPO_REPORTE)
    return qry


def q_transformar_combos_bonos_fuente_3(vSChema, TABLA):
    qry = """
    SELECT r.fecha_proceso,
        r.telefono,
        r.valor_recarga_base,
        a.fecha_alta_im0,
        a.fecha_alta_fm0,
        a.fecha_alta_im1,
        a.fecha_alta_fm1,
        a.fecha_alta_im2,
        a.fecha_alta_fm2,
        a.fecha_alta_im3,
        a.fecha_alta_fm3,
        a.fecha_alta_im4,
        a.fecha_alta_fm4,
        a.fecha_alta_im5,
        a.fecha_alta_fm5,
        a.fecha_alta
    FROM {vSChema}.{TABLA} r 
    INNER JOIN {vSChema}.universo_altas_traficadoras_fuente_final a 
        ON a.telefono = r.telefono
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_generar_reporte_combos_bonos_resumido(tipo_reporte, vSChema, TABLA, COLUMNA_FECHA_INICIO, COLUMNA_FECHA_FIN, MES):
    qry = """
    SELECT telefono,
        fecha_alta,
        COUNT(*) AS cant_{tipo_reporte}_M{MES},
        SUM(valor_recarga_base) AS valor_{tipo_reporte}_M{MES}
    FROM {vSChema}.{TABLA}
    WHERE fecha_proceso >= {COLUMNA_FECHA_INICIO}
        AND fecha_proceso <= {COLUMNA_FECHA_FIN}
    GROUP BY telefono, fecha_alta
    """.format(
        tipo_reporte=tipo_reporte,
        vSChema=vSChema,
        TABLA=TABLA,
        COLUMNA_FECHA_INICIO=COLUMNA_FECHA_INICIO,
        COLUMNA_FECHA_FIN=COLUMNA_FECHA_FIN,
        MES=MES
    )
    return qry


def q_transformar_recargas_fuente(vSChema, TABLA):
    qry = """
    SELECT fecha_proceso,
        telefono,
        valor_recarga_base,
        origen_recarga_aa
    FROM {vSChema}.{TABLA}
    WHERE rec_pkt = 'REC'
        AND operadora <> 'TUENTI'
        AND TIPO_TRANSACCION = 'ACTIVA'
        AND ESTADO_RECARGA = 'RECARGA'
        OR (
            pais_destino = 'EC'
            AND usuario_responsablecd = 'bcgi'
            AND plataforma = 'IRP'
        )
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_transformar_recargas_fuente_2(vSChema, TABLA):
    qry = """
    SELECT r.fecha_proceso,
        r.telefono,
        r.valor_recarga_base
    FROM {vSChema}.{TABLA} r
    INNER JOIN db_altamira.par_origen_recarga ori 
        ON ori.ORIGENRECARGAID = r.origen_recarga_aa
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_transformar_recargas_fuente_3(vSChema, TABLA):
    qry = """
    SELECT r.fecha_proceso,
        r.telefono,
        r.valor_recarga_base,
        a.fecha_alta_im0,
        a.fecha_alta_fm0,
        a.fecha_alta_im1,
        a.fecha_alta_fm1,
        a.fecha_alta_im2,
        a.fecha_alta_fm2,
        a.fecha_alta_im3,
        a.fecha_alta_fm3,
        a.fecha_alta_im4,
        a.fecha_alta_fm4,
        a.fecha_alta_im5,
        a.fecha_alta_fm5,
        a.fecha_alta
    FROM {vSChema}.{TABLA} r
    INNER JOIN {vSChema}.universo_altas_traficadoras_fuente_final a 
        ON a.telefono = r.telefono
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_generar_reporte_recargas_resumido(vSChema, TABLA, COLUMNA_FECHA_INICIO, COLUMNA_FECHA_FIN, MES):
    qry = """
    SELECT telefono,
        fecha_alta,
        COUNT(*) AS cant_rec_M{MES},
        SUM(valor_recarga_base) AS valor_rec_M{MES}
    FROM {vSChema}.{TABLA}
    WHERE fecha_proceso >= {COLUMNA_FECHA_INICIO} 
        AND fecha_proceso <= {COLUMNA_FECHA_FIN}
    GROUP BY telefono, fecha_alta
    """.format(
        vSChema=vSChema,
        TABLA=TABLA,
        COLUMNA_FECHA_INICIO=COLUMNA_FECHA_INICIO,
        COLUMNA_FECHA_FIN=COLUMNA_FECHA_FIN,
        MES=MES
    )
    return qry


def q_generar_reporte_recargas_combos_bonos(vSChema, TABLA):
    qry = """
    SELECT telefono,
        fecha_alta,
        cant_rec_M0,
        cant_bono_M0,
        cant_combo_M0,
        valor_rec_M0,
        valor_bono_M0,
        valor_combo_M0,
        cant_rec_M1,
        cant_bono_M1,
        cant_combo_M1,
        valor_rec_M1,
        valor_bono_M1,
        valor_combo_M1,
        cant_rec_M2,
        cant_bono_M2,
        cant_combo_M2,
        valor_rec_M2,
        valor_bono_M2,
        valor_combo_M2,
        cant_rec_M3,
        cant_bono_M3,
        cant_combo_M3,
        valor_rec_M3,
        valor_bono_M3,
        valor_combo_M3,
        cant_rec_M4,
        cant_bono_M4,
        cant_combo_M4,
        valor_rec_M4,
        valor_bono_M4,
        valor_combo_M4,
        cant_rec_M5,
        cant_bono_M5,
        cant_combo_M5,
        valor_rec_M5,
        valor_bono_M5,
        valor_combo_M5
    FROM {vSChema}.{TABLA}
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry


def q_generar_trafico_datos_fuente(FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT numeroorigen as telefono,
        vol_total_2g,
        vol_total_3g,
        vol_total_lte,
        vol_total_otro,
        activity_start_dt as fecha_proceso
    FROM db_cmd.otc_t_dm_cur_t2 
    WHERE activity_start_dt >= {FECHA_INICIO} 
        AND activity_start_dt <= {FECHA_FIN}
    """.format(
            FECHA_INICIO=FECHA_INICIO,
            FECHA_FIN=FECHA_FIN
        )
    return qry


def q_transformar_trafico_datos_fuente(vSChema, TABLA):
    qry = """
    SELECT td.telefono,
        td.vol_total_2g,
        td.vol_total_3g,
        td.vol_total_lte,
        td.vol_total_otro,
        td.fecha_proceso,
        a.fecha_alta_im0,
        a.fecha_alta_fm0,
        a.fecha_alta_im1,
        a.fecha_alta_fm1,
        a.fecha_alta_im2,
        a.fecha_alta_fm2,
        a.fecha_alta_im3,
        a.fecha_alta_fm3,
        a.fecha_alta_im4,
        a.fecha_alta_fm4,
        a.fecha_alta_im5,
        a.fecha_alta_fm5,
        a.fecha_alta
    FROM {vSChema}.{TABLA} td
    INNER JOIN {vSChema}.universo_altas_traficadoras_fuente_final a 
        ON a.telefono = td.telefono
    """.format(vSChema=vSChema, TABLA=TABLA)
    return qry 
    

def q_generar_trafico_datos_resumido(vSChema, TABLA, COLUMNA_FECHA_INICIO, COLUMNA_FECHA_FIN):
    qry = """
    SELECT telefono,
        fecha_alta,
        CAST(SUM(COALESCE(vol_total_2g, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_2g,
        CAST(SUM(COALESCE(vol_total_3g, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_3g,
        CAST(SUM(COALESCE(vol_total_lte, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_lte,
        CAST(SUM(COALESCE(vol_total_otro, 0)/1024/1024) AS decimal(19, 2)) AS total_trafico_otro
    FROM {vSChema}.{TABLA}
    WHERE fecha_proceso >= {COLUMNA_FECHA_INICIO} 
        AND fecha_proceso <= {COLUMNA_FECHA_FIN}
    GROUP BY telefono, fecha_alta
    """.format(
        vSChema=vSChema,
        TABLA=TABLA,
        COLUMNA_FECHA_INICIO=COLUMNA_FECHA_INICIO,
        COLUMNA_FECHA_FIN=COLUMNA_FECHA_FIN 
    )
    return qry


def q_generar_trafico_voz_fuente(FECHA_INICIO, FECHA_FIN):
    qry = """
    SELECT a_direction_number,
        b_direction_number,
        duracion,
        fecha_evento_int as fecha_proceso
    FROM db_trafica.otc_t_cur_voz_trafica 
    WHERE fecha_evento_int >= {FECHA_INICIO} 
        AND fecha_evento_int <= {FECHA_FIN}
    """.format(
        FECHA_INICIO=FECHA_INICIO,
        FECHA_FIN=FECHA_FIN
    )
    return qry


def q_transformar_trafico_voz_fuente(vSChema, TABLA, COLUMNA):
    qry = """
    SELECT tv.{COLUMNA} AS telefono,
        tv.duracion,
        tv.fecha_proceso,
        a.fecha_alta_im0,
        a.fecha_alta_fm0,
        a.fecha_alta_im1,
        a.fecha_alta_fm1,
        a.fecha_alta_im2,
        a.fecha_alta_fm2,
        a.fecha_alta_im3,
        a.fecha_alta_fm3,
        a.fecha_alta_im4,
        a.fecha_alta_fm4,
        a.fecha_alta_im5,
        a.fecha_alta_fm5,
        a.fecha_alta
    FROM {vSChema}.{TABLA} tv
    INNER JOIN {vSChema}.universo_altas_traficadoras_fuente_final a 
        ON a.telefono = tv.{COLUMNA}
    """.format(
        vSChema=vSChema, 
        TABLA=TABLA,
        COLUMNA=COLUMNA
    )
    return qry 


def q_generar_trafico_voz_resumido(vSChema, TABLA, COLUMNA_FECHA_INICIO, COLUMNA_FECHA_FIN, sentido, MES):
    qry = """
    SELECT telefono,
        fecha_alta,
        SUM(ABS(CAST(COALESCE(duracion, 0) AS int))) AS trafico_{sentido}_voz_M{MES}
    FROM {vSChema}.{TABLA}
    WHERE fecha_proceso >= {COLUMNA_FECHA_INICIO} 
        AND fecha_proceso <= {COLUMNA_FECHA_FIN}
	GROUP BY telefono, fecha_alta
    """.format(
        vSChema=vSChema,
        TABLA=TABLA,
        sentido=sentido,
        COLUMNA_FECHA_INICIO=COLUMNA_FECHA_INICIO,
        COLUMNA_FECHA_FIN=COLUMNA_FECHA_FIN,
        MES=MES
    )
    return qry


def identificar_lineas_traficadoras(vSChema, TABLA):
    qry = """
    SELECT telefono,
        fecha_alta,
        CASE
            WHEN trafico_datos_M0 < 5
                AND trafico_entrante_voz_M0 <= 60
                AND trafico_saliente_voz_M0 <= 60 THEN 0
            ELSE 1
        END as traficador_M0,
        CASE
            WHEN trafico_datos_M1 < 5
                AND trafico_entrante_voz_M1 <= 60
                AND trafico_saliente_voz_M1 <= 60 THEN 0
            ELSE 1
        END as traficador_M1,
        CASE
            WHEN trafico_datos_M2 < 5
                AND trafico_entrante_voz_M2 <= 60
                AND trafico_saliente_voz_M2 <= 60 THEN 0
            ELSE 1
        END as traficador_M2,
        CASE
            WHEN trafico_datos_M3 < 5
                AND trafico_entrante_voz_M3 <= 60
                AND trafico_saliente_voz_M3 <= 60 THEN 0
            ELSE 1
        END as traficador_M3,
        CASE
            WHEN trafico_datos_M4 < 5
                AND trafico_entrante_voz_M4 <= 60
                AND trafico_saliente_voz_M4 <= 60 THEN 0
            ELSE 1
        END as traficador_M4,
        CASE
            WHEN trafico_datos_M5 < 5
                AND trafico_entrante_voz_M5 <= 60
                AND trafico_saliente_voz_M5 <= 60 THEN 0
            ELSE 1
        END as traficador_M5
    FROM {vSChema}.{TABLA}
    """.format(
        vSChema=vSChema, 
        TABLA=TABLA
    )
    return qry


def unir_universo_altas_con_lineas_traficadoras_y_trafico(vSChema, vSChemaTmp, FECHA_EJECUCION):
    qry = """
    INSERT OVERWRITE TABLE {vSChema}.otc_t_calidad_altas_traficadoras PARTITION(fecha_proceso={FECHA_EJECUCION})
    SELECT u.movimiento,
        u.telefono,
        u.fecha_alta,
        u.mes,
        u.anio,
        u.linea_negocio,
        u.marca,
        u.distribuidor,
        u.codigo_da,
        u.nombre_plaza,
        u.canal_comercial,
        u.sub_canal,
        u.provincia_ivr,
        u.provincia_trafico,
        u.ciudad,
        u.operadora_origen,
        u.jefe_de_venta,
        u.usuario,
        lt.traficador_m0,
        rcb.cant_rec_m0,
        rcb.cant_bono_m0,
        rcb.cant_combo_m0,
        rcb.valor_rec_m0,
        rcb.valor_bono_m0,
        rcb.valor_combo_m0,
        lt.traficador_m1,
        rcb.cant_rec_m1,
        rcb.cant_bono_m1,
        rcb.cant_combo_m1,
        rcb.valor_rec_m1,
        rcb.valor_bono_m1,
        rcb.valor_combo_m1,
        lt.traficador_m2,
        rcb.cant_rec_m2,
        rcb.cant_bono_m2,
        rcb.cant_combo_m2,
        rcb.valor_rec_m2,
        rcb.valor_bono_m2,
        rcb.valor_combo_m2,
        lt.traficador_m3,
        rcb.cant_rec_m3,
        rcb.cant_bono_m3,
        rcb.cant_combo_m3,
        rcb.valor_rec_m3,
        rcb.valor_bono_m3,
        rcb.valor_combo_m3,
        lt.traficador_m4,
        rcb.cant_rec_m4,
        rcb.cant_bono_m4,
        rcb.cant_combo_m4,
        rcb.valor_rec_m4,
        rcb.valor_bono_m4,
        rcb.valor_combo_m4,
        lt.traficador_m5,
        rcb.cant_rec_m5, 
        rcb.cant_bono_m5, 
        rcb.cant_combo_m5, 
        rcb.valor_rec_m5, 
        rcb.valor_bono_m5, 
        rcb.valor_combo_m5 
    FROM {vSChemaTmp}.universo_altas_traficadoras_fuente_final u
    LEFT JOIN {vSChemaTmp}.otc_t_reporte_recargas_combos_bonos_final_cat rcb
        ON u.telefono = rcb.telefono AND u.fecha_alta = rcb.fecha_alta
    LEFT JOIN {vSChemaTmp}.otc_t_lineas_traficadoras_final_cat lt
        ON u.telefono = lt.telefono AND u.fecha_alta = lt.fecha_alta
    """.format(
        vSChema=vSChema, 
        vSChemaTmp=vSChemaTmp,
        FECHA_EJECUCION=FECHA_EJECUCION
    )
    return qry


# def q_insertar_calidad_altas_traficadoras(vSChema, vSChemaTmp, FECHA_EJECUCION):
# 	qry = """
#     INSERT OVERWRITE TABLE {vSChema}.otc_t_calidad_altas_traficadoras PARTITION(fecha_proceso={FECHA_EJECUCION})
#     SELECT movimiento,
#         telefono,
#         fecha_alta,
#         mes,
#         anio,
#         linea_negocio,
#         marca,
#         distribuidor,
#         codigo_da,
#         nombre_plaza,
#         canal_comercial,
#         sub_canal,
#         provincia_ivr,
#         provincia_trafico,
#         ciudad,
#         operadora_origen,
#         jefe_de_venta,
#         usuario,
#         traficador_m0,
#         cant_rec_M0,
#         cant_bono_M0,
#         cant_combo_M0,
#         valor_rec_M0,
#         valor_bono_M0,
#         valor_combo_M0,
#         traficador_m1,
#         cant_rec_M1,
#         cant_bono_M1,
#         cant_combo_M1,
#         valor_rec_M1,
#         valor_bono_M1,
#         valor_combo_M1,
#         traficador_m2,
#         cant_rec_M2,
#         cant_bono_M2,
#         cant_combo_M2,
#         valor_rec_M2,
#         valor_bono_M2,
#         valor_combo_M2,
#         traficador_m3,
#         cant_rec_M3,
#         cant_bono_M3,
#         cant_combo_M3,
#         valor_rec_M3,
#         valor_bono_M3,
#         valor_combo_M3,
#         traficador_m4,
#         cant_rec_M4,
#         cant_bono_M4,
#         cant_combo_M4,
#         valor_rec_M4,
#         valor_bono_M4,
#         valor_combo_M4,
#         traficador_m5,
#         cant_rec_M5,
#         cant_bono_M5,
#         cant_combo_M5,
#         valor_rec_M5,
#         valor_bono_M5,
#         valor_combo_M5
#     FROM {vSChemaTmp}.otc_t_calidad_altas_traficadoras_diario
#        	""".format(
#             vSChema=vSChema, 
#             vSChemaTmp=vSChemaTmp,
#             FECHA_EJECUCION=FECHA_EJECUCION
#         )
# 	return qry


def q_generar_reporte_calidad_altas_traficadoras(vSChema, FECHA_EJECUCION):
	qry="""
    SELECT movimiento,
        telefono,
        fecha_alta,
        mes,
        anio,
        linea_negocio,
        marca,
        distribuidor,
        codigo_da,
        nombre_plaza,
        canal_comercial,
        sub_canal,
        provincia_ivr,
        provincia_trafico,
        ciudad,
        operadora_origen,
        jefe_de_venta,
        usuario,
        traficador_m0,
        cant_rec_M0,
        cant_bono_M0,
        cant_combo_M0,
        valor_rec_M0,
        valor_bono_M0,
        valor_combo_M0,
        traficador_m1,
        cant_rec_M1,
        cant_bono_M1,
        cant_combo_M1,
        valor_rec_M1,
        valor_bono_M1,
        valor_combo_M1,
        traficador_m2,
        cant_rec_M2,
        cant_bono_M2,
        cant_combo_M2,
        valor_rec_M2,
        valor_bono_M2,
        valor_combo_M2,
        traficador_m3,
        cant_rec_M3,
        cant_bono_M3,
        cant_combo_M3,
        valor_rec_M3,
        valor_bono_M3,
        valor_combo_M3,
        traficador_m4,
        cant_rec_M4,
        cant_bono_M4,
        cant_combo_M4,
        valor_rec_M4,
        valor_bono_M4,
        valor_combo_M4,
        traficador_m5,
        cant_rec_M5,
        cant_bono_M5,
        cant_combo_M5,
        valor_rec_M5,
        valor_bono_M5,
        valor_combo_M5,
        fecha_proceso
    FROM {vSChema}.otc_t_calidad_altas_traficadoras
    -- WHERE fecha_proceso = {FECHA_EJECUCION}
    """.format(vSChema=vSChema, FECHA_EJECUCION=FECHA_EJECUCION)
	return qry
