#!/bin/bash
#########################################################################################################
# NOMBRE: SH_CALIDAD_ALTAS_TRAFICADORAS.sh	      								                        #
# DESCRIPCION: Shell que calcula el proceso de Calidad Altas Traficadoras				                #
# AUTOR: Cesar Andrade                            							                            #
# FECHA CREACION: 2023-07-21   										                                    #
# PARAMETROS DEL SHELL                            					    		                        #
# FECHA_EJECUCION del proceso  												    	                    #
#########################################################################################################
# MODIFICACIONES											                                            #
# FECHA  		AUTOR     		DESCRIPCION MOTIVO					                                    #
#########################################################################################################
set -e

FECHA_EJECUCION=$1 # 20230901

ENTIDAD=D_RPRTCLDDLTSTR0010
AMBIENTE=0 # AMBIENTE (1=produccion, 0=desarrollo)

if [ $AMBIENTE -gt 0 ]; then
    TABLA=params
else
    TABLA=params_des
fi

VAL_RUTA=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA';")
VAL_PYTHON=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_PYTHON';")
VAL_RUTA_SPARK=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_SPARK';")
ESQUEMA=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA';")
ESQUEMA_TMP=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'ESQUEMA_TMP';")
ETAPA=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'ETAPA';")
VAL_COLA_EJECUCION=$(mysql -N <<<"select valor from $TABLA where entidad = '"$ENTIDAD"' AND parametro = 'VAL_COLA_EJECUCION';")
VAL_NOMBRE_ARCH_1=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOMBRE_ARCH_1';")
VAL_NOMBRE_ARCH_2=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOMBRE_ARCH_2';")
VAL_NOMBRE_ARCH_3=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NOMBRE_ARCH_3';")
VAL_RUTA_HDFS_1=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_HDFS_1';")
VAL_RUTA_HDFS_2=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_RUTA_HDFS_2';")

#------------------------------------------------------
# PARAMETROS SPARK
#------------------------------------------------------
VAL_RUTA_SPARK=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_RUTA_SPARK';")
VAL_MASTER=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_MASTER';")
VAL_DRIVER_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_DRIVER_MEMORY';")
VAL_EXECUTOR_MEMORY=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_EXECUTOR_MEMORY';")
VAL_NUM_EXECUTORS=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS';")
VAL_NUM_EXECUTORS_CORES=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_NUM_EXECUTORS_CORES';")
VAL_KINIT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SPARK_GENERICO' AND parametro = 'VAL_KINIT';")
$VAL_KINIT

#------------------------------------------------------
# PARAMETROS ACCESO SFTP
#------------------------------------------------------
SFTP_GENERICO_SH=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'SFTP_GENERICO_SH';")
VAL_SFTP_HOST_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_HOST';")
VAL_SFTP_PORT_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PORT';")
VAL_SFTP_USER_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_USER_DDATOS';")
VAL_SFTP_PASS_OUT=$(mysql -N <<<"select valor from params where ENTIDAD = 'SFTP_GENERICO' AND parametro = 'VAL_SFTP_PASS_DDATOS';")
VAL_SFTP_RUTA_OUT=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_OUT';")
VAL_SFTP_RUTA_IN=$(mysql -N <<<"select valor from $TABLA where ENTIDAD = '"$ENTIDAD"' AND parametro = 'VAL_SFTP_RUTA_IN';")

FECHA_HORA=$(date '+%Y%m%d%H%M%S')
VAL_LOG=$VAL_RUTA/log/"$ENTIDAD"_"$FECHA_HORA".log
VAL_LOCAL_RUTA_IN=$VAL_RUTA/input
VAL_LOCAL_RUTA_OUT=$VAL_RUTA/output

FECHA_EJECUCION_ANTERIOR=$(date '+%Y%m%d' -d "$FECHA_EJECUCION-1 day")

FECHA_INICIO=$(date '+%Y%m%d' -d "$FECHA_EJECUCION-5 month")
ANIO_FECHA_INICIO=$(date '+%Y' -d "$FECHA_INICIO")
ANIO_FECHA_EJECUCION=$(date '+%Y' -d "$FECHA_EJECUCION")
FECHA_ACTUAL=$(date '+%Y%m%d' -d "$FECHA_EJECUCION+5 month")

if [ $FECHA_ACTUAL -gt $(date '+%Y%m%d') ]; then
    FECHA_ACTUAL=$(date '+%Y%m%d')
fi

DIA_EJECUCION=$(date '+%d' -d "$FECHA_EJECUCION")

if [ $ANIO_FECHA_INICIO -lt $ANIO_FECHA_EJECUCION ]; then
    FECHA_INICIO=$ANIO_FECHA_EJECUCION"0101"
fi

if [ $DIA_EJECUCION = "01" ]; then
    FECHA_13_MESES_ATRAS=$(date '+%Y%m%d' -d "$FECHA_EJECUCION-13 month")
	FECHA_13_MESES_ATRAS=$(date '+%Y%m' -d "$FECHA_13_MESES_ATRAS")"01"

    FECHA_PROCESO=$(date '+%Y%m%d' -d "$FECHA_EJECUCION_ANTERIOR")

    FECHA_FIN_MES_1=$(date '+%Y%m' -d "$FECHA_EJECUCION-5 month")"01"
    FECHA_FIN_MES_2=$(date '+%Y%m' -d "$FECHA_EJECUCION-4 month")"01"
    FECHA_FIN_MES_3=$(date '+%Y%m' -d "$FECHA_EJECUCION-3 month")"01"
    FECHA_FIN_MES_4=$(date '+%Y%m' -d "$FECHA_EJECUCION-2 month")"01"
    FECHA_FIN_MES_5=$(date '+%Y%m' -d "$FECHA_EJECUCION-1 month")"01"
else
    FECHA_13_MESES_ATRAS=$FECHA_EJECUCION

    FECHA_PROCESO=$(date '+%Y%m' -d "$FECHA_EJECUCION+1 month")"01"
    FECHA_PROCESO=$(date '+%Y%m%d' -d "$FECHA_PROCESO-1 day")

    FECHA_FIN_MES_1=$(date '+%Y%m' -d "$FECHA_EJECUCION-4 month")"01"
    FECHA_FIN_MES_2=$(date '+%Y%m' -d "$FECHA_EJECUCION-3 month")"01"
    FECHA_FIN_MES_3=$(date '+%Y%m' -d "$FECHA_EJECUCION-2 month")"01"
    FECHA_FIN_MES_4=$(date '+%Y%m' -d "$FECHA_EJECUCION-1 month")"01"
    FECHA_FIN_MES_5=$(date '+%Y%m' -d "$FECHA_EJECUCION")"01"
fi

if [ $DIA_EJECUCION = "02" ]; then
    ELIMINAR_PARTICION_PREVIA="NO"
else
    ELIMINAR_PARTICION_PREVIA="SI"
fi

# VALIDACION DE PARAMETROS INICIALES
if [ -z "$ENTIDAD" ] || 
	[ -z "$VAL_RUTA" ] || 
	[ -z "$VAL_PYTHON" ] || 
    [ -z "$ESQUEMA" ] || 
	[ -z "$ESQUEMA_TMP" ] || 
	[ -z "$VAL_NOMBRE_ARCH_1" ] || 
	[ -z "$VAL_NOMBRE_ARCH_2" ] || 
	[ -z "$VAL_NOMBRE_ARCH_3" ] ||
	[ -z "$VAL_RUTA_HDFS_1" ] ||
	[ -z "$VAL_RUTA_HDFS_2" ] ||
	[ -z "$VAL_RUTA_SPARK" ] ||
	[ -z "$VAL_MASTER" ] ||
	[ -z "$VAL_DRIVER_MEMORY" ] ||
	[ -z "$VAL_EXECUTOR_MEMORY" ] ||
	[ -z "$VAL_NUM_EXECUTORS" ] ||
	[ -z "$VAL_NUM_EXECUTORS_CORES" ] ||
	[ -z "$VAL_KINIT" ] ||
	[ -z "$SFTP_GENERICO_SH" ] || 
	[ -z "$VAL_SFTP_HOST_OUT" ] || 
    [ -z "$VAL_SFTP_PORT_OUT" ] || 
	[ -z "$VAL_SFTP_USER_OUT" ] || 
	[ -z "$VAL_SFTP_PASS_OUT" ] ||
	[ -z "$VAL_SFTP_RUTA_OUT" ] ||
	[ -z "$VAL_SFTP_RUTA_IN" ]; then
    echo " ERROR: - uno de los parametros esta vacio o nulo" >>"$VAL_LOG"
    exit 1
fi

#------------------------------------------------------
# IMPRESION PARAMETROS
#------------------------------------------------------
echo "FECHA_EJECUCION: $FECHA_EJECUCION" >>$VAL_LOG
echo "VAL_RUTA: $VAL_RUTA" >>$VAL_LOG
echo "ESQUEMA: $ESQUEMA" >>$VAL_LOG
echo "ESQUEMA_TMP: $ESQUEMA_TMP" >>$VAL_LOG
echo "VAL_COLA_EJECUCION: $VAL_COLA_EJECUCION" >>$VAL_LOG
echo "ETAPA: $ETAPA" >>$VAL_LOG
echo "VAL_NOMBRE_ARCH_1: $VAL_NOMBRE_ARCH_1" >>$VAL_LOG
echo "VAL_NOMBRE_ARCH_2: $VAL_NOMBRE_ARCH_2" >>$VAL_LOG
echo "VAL_NOMBRE_ARCH_3: $VAL_NOMBRE_ARCH_3" >>$VAL_LOG
echo "VAL_RUTA_HDFS_1: $VAL_RUTA_HDFS_1" >>$VAL_LOG
echo "VAL_RUTA_HDFS_2: $VAL_RUTA_HDFS_2" >>$VAL_LOG
echo "VAL_RUTA_SPARK: $VAL_RUTA_SPARK" >>$VAL_LOG
echo "VAL_MASTER: $VAL_MASTER" >>$VAL_LOG
echo "VAL_DRIVER_MEMORY: $VAL_DRIVER_MEMORY" >>$VAL_LOG
echo "VAL_EXECUTOR_MEMORY: $VAL_EXECUTOR_MEMORY" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS: $VAL_NUM_EXECUTORS" >>$VAL_LOG
echo "VAL_NUM_EXECUTORS_CORES: $VAL_NUM_EXECUTORS_CORES" >>$VAL_LOG
echo "VAL_KINIT: $VAL_KINIT" >>$VAL_LOG
echo "SFTP_GENERICO_SH: $SFTP_GENERICO_SH" >>$VAL_LOG
echo "VAL_SFTP_HOST_OUT: $VAL_SFTP_HOST_OUT" >>$VAL_LOG
echo "VAL_SFTP_PORT_OUT: $VAL_SFTP_PORT_OUT" >>$VAL_LOG
echo "VAL_SFTP_USER_OUT: $VAL_SFTP_USER_OUT" >>$VAL_LOG
echo "VAL_SFTP_PASS_OUT: $VAL_SFTP_PASS_OUT" >>$VAL_LOG
echo "VAL_SFTP_RUTA_OUT: $VAL_SFTP_RUTA_OUT" >>$VAL_LOG
echo "VAL_SFTP_RUTA_IN: $VAL_SFTP_RUTA_IN" >>$VAL_LOG
echo "VAL_LOCAL_RUTA_OUT: $VAL_LOCAL_RUTA_OUT" >>$VAL_LOG
echo "VAL_LOCAL_RUTA_IN: $VAL_LOCAL_RUTA_IN" >>$VAL_LOG
echo "FECHA_EJECUCION_ANTERIOR: $FECHA_EJECUCION_ANTERIOR" >>$VAL_LOG
echo "FECHA_INICIO: $FECHA_INICIO" >>$VAL_LOG
echo "FECHA_ACTUAL: $FECHA_ACTUAL" >>$VAL_LOG
echo "FECHA_13_MESES_ATRAS: $FECHA_13_MESES_ATRAS" >>$VAL_LOG
echo "FECHA_PROCESO: $FECHA_PROCESO" >>$VAL_LOG
echo "FECHA_FIN_MES_1: $FECHA_FIN_MES_1" >>$VAL_LOG
echo "FECHA_FIN_MES_2: $FECHA_FIN_MES_2" >>$VAL_LOG
echo "FECHA_FIN_MES_3: $FECHA_FIN_MES_3" >>$VAL_LOG
echo "FECHA_FIN_MES_4: $FECHA_FIN_MES_4" >>$VAL_LOG
echo "FECHA_FIN_MES_5: $FECHA_FIN_MES_5" >>$VAL_LOG
echo "ELIMINAR_PARTICION_PREVIA: $ELIMINAR_PARTICION_PREVIA" >>$VAL_LOG
echo "VAL_LOG: $VAL_LOG" >>$VAL_LOG

if [ "$ETAPA" = "1" ]; then
    VAL_SFTP_RUTA_IN=$(echo $VAL_SFTP_RUTA_IN | sed "s/~}</ /g")
    VAL_SFTP_RUTA_IN=$(echo $VAL_SFTP_RUTA_IN | tr '"' "'")

	VAL_MODO=1
    VAL_BANDERA_FTP=0

	VAL_NOM_FILE_IN=$VAL_NOMBRE_ARCH_1
	
    sh -x $SFTP_GENERICO_SH \
        $VAL_MODO $VAL_BANDERA_FTP \
        $VAL_SFTP_USER_OUT $VAL_SFTP_PASS_OUT $VAL_SFTP_HOST_OUT $VAL_SFTP_PORT_OUT "$VAL_SFTP_RUTA_IN" \
        $VAL_NOM_FILE_IN $VAL_LOCAL_RUTA_IN $VAL_LOG

    VAL_ERRORES=$(egrep 'ERROR - En la transferencia del archivo' $VAL_LOG | wc -l)

    if [ $VAL_ERRORES -ne 0 ]; then
        echo "==== ERROR en la transferencia FTP ====" >>$VAL_LOG
        exit 1
    else
        echo "==== FIN PROCESO IMPORTACION FTP ====" >>$VAL_LOG
    fi

    VAL_NOM_FILE_IN=$VAL_NOMBRE_ARCH_2

    sh -x $SFTP_GENERICO_SH \
        $VAL_MODO $VAL_BANDERA_FTP \
        $VAL_SFTP_USER_OUT $VAL_SFTP_PASS_OUT $VAL_SFTP_HOST_OUT $VAL_SFTP_PORT_OUT "$VAL_SFTP_RUTA_IN" \
        "$VAL_NOM_FILE_IN" $VAL_LOCAL_RUTA_IN $VAL_LOG

    VAL_ERRORES=$(egrep 'ERROR - En la transferencia del archivo' $VAL_LOG | wc -l)

    if [ $VAL_ERRORES -ne 0 ]; then
        echo "==== ERROR en la transferencia FTP ====" >>$VAL_LOG
        exit 1
    else
        echo "==== FIN PROCESO IMPORTACION FTP ====" >>$VAL_LOG
    fi

    # seteo de etapa
    echo "Procesado ETAPA 1" &>>$VAL_LOG
    # $(mysql -N <<<"update $TABLA set valor='2' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
	ETAPA="2"
fi

if [ "$ETAPA" = "2" ]; then
	VAL_FILE_XLS=$VAL_NOMBRE_ARCH_1
	VAL_FILE_CSV="${VAL_NOMBRE_ARCH_1::-4}"csv

	$VAL_RUTA_SPARK \
    $VAL_RUTA/python/read_file_carga_csv.py \
        --ruta=$VAL_LOCAL_RUTA_IN \
        --file="$VAL_FILE_XLS" \
        --sep="," 2>&1 &>>$VAL_LOG
	
    hdfs dfs -rm -f $VAL_RUTA_HDFS_1/$VAL_FILE_CSV >> $VAL_LOG
	
    hdfs dfs -put $VAL_LOCAL_RUTA_IN/$VAL_FILE_CSV $VAL_RUTA_HDFS_1/ >> $VAL_LOG

	$VAL_RUTA_SPARK \
    	$VAL_RUTA/python/otc_t_jefes_comerciales_import.py \
        --vSFile=$VAL_RUTA_HDFS_1/$VAL_FILE_CSV \
        --vSChema=$ESQUEMA_TMP \
        --vSTable="otc_t_jefe_comercial_2" \
        --vSEntidad=$ENTIDAD \
        --vIEtapa=$ETAPA 2>&1 &>>$VAL_LOG

    # seteo de etapa
    echo "Procesado ETAPA 2" &>>$VAL_LOG
    # $(mysql -N <<<"update $TABLA set valor='3' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
	ETAPA="3"
fi

if [ "$ETAPA" = "3" ]; then
	VAL_FILE_XLS=$VAL_NOMBRE_ARCH_2
	VAL_FILE_CSV="${VAL_NOMBRE_ARCH_2::-4}"csv
	VAL_FILE_CSV=$(echo "$VAL_FILE_CSV" | sed 's/ //')

	$VAL_RUTA_SPARK \
    	$VAL_RUTA/python/read_file_carga_csv.py \
        --ruta=$VAL_LOCAL_RUTA_IN \
        --file="$VAL_FILE_XLS" \
        --sep="," 2>&1 &>>$VAL_LOG
    
	hdfs dfs -rm -f $VAL_RUTA_HDFS_2/$VAL_FILE_CSV >> $VAL_LOG
	
    hdfs dfs -put $VAL_LOCAL_RUTA_IN/$VAL_FILE_CSV $VAL_RUTA_HDFS_2/ >> $VAL_LOG

	$VAL_RUTA_SPARK \
    	$VAL_RUTA/python/otc_t_vendedores_import.py \
        --vSFile=$VAL_RUTA_HDFS_2/$VAL_FILE_CSV \
        --vSChema=$ESQUEMA_TMP \
        --vSTable="otc_t_vendedores_2" \
        --vSEntidad=$ENTIDAD \
        --vIEtapa=$ETAPA 2>&1 &>>$VAL_LOG

    # seteo de etapa
    echo "Procesado ETAPA 3" &>>$VAL_LOG
    # $(mysql -N <<<"update $TABLA set valor='4' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
	ETAPA="4"
fi

if [ "$ETAPA" = "4" ]; then
    
	$VAL_RUTA_SPARK \
        --name $ENTIDAD \
        --queue $VAL_COLA_EJECUCION \
	    --conf spark.port.maxRetries=100 \
        --conf spark.rpc.message.maxSize=128 \
        --master $VAL_MASTER \
        --driver-memory $VAL_DRIVER_MEMORY \
        --executor-memory $VAL_EXECUTOR_MEMORY \
        --num-executors $VAL_NUM_EXECUTORS \
        --executor-cores $VAL_NUM_EXECUTORS_CORES \
        $VAL_RUTA/python/$VAL_PYTHON \
	    --vSEntidad=$ENTIDAD \
        --vSChema=$ESQUEMA \
        --vSChemaTmp=$ESQUEMA_TMP \
        --FECHA_EJECUCION=$FECHA_EJECUCION \
        --FECHA_INICIO=$FECHA_INICIO \
        --FECHA_ACTUAL=$FECHA_ACTUAL \
        --FECHA_PROCESO=$FECHA_PROCESO \
        --FECHA_EJECUCION_ANTERIOR=$FECHA_EJECUCION_ANTERIOR \
        --FECHA_13_MESES_ATRAS=$FECHA_13_MESES_ATRAS \
        --ELIMINAR_PARTICION_PREVIA=$ELIMINAR_PARTICION_PREVIA \
        --FECHA_FIN_MES_1=$FECHA_FIN_MES_1 \
        --FECHA_FIN_MES_2=$FECHA_FIN_MES_2 \
        --FECHA_FIN_MES_3=$FECHA_FIN_MES_3 \
        --FECHA_FIN_MES_4=$FECHA_FIN_MES_4 \
        --FECHA_FIN_MES_5=$FECHA_FIN_MES_5 2>&1 &>>$VAL_LOG 

    # seteo de etapa
    echo "Procesado ETAPA 4" &>>$VAL_LOG
    # $(mysql -N <<<"update $TABLA set valor='5' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
	ETAPA="5"
fi

if [ "$ETAPA" = "5" ]; then

	$VAL_RUTA_SPARK \
	    --name $ENTIDAD \
        --queue $VAL_COLA_EJECUCION \
        --master $VAL_MASTER \
        --driver-memory $VAL_DRIVER_MEMORY \
        --executor-memory $VAL_EXECUTOR_MEMORY \
        --num-executors $VAL_NUM_EXECUTORS \
        --executor-cores $VAL_NUM_EXECUTORS_CORES \
        $VAL_RUTA/python/generar_reporte_csv.py \
        --vSEntidad=$ENTIDAD \
        --vSChema=$ESQUEMA \
        --FECHA_EJECUCION=$FECHA_EJECUCION \
        --RUTA_CSV=$VAL_LOCAL_RUTA_OUT/$VAL_NOMBRE_ARCH_3 2>&1 &>>$VAL_LOG

	rm -f $VAL_LOCAL_RUTA_OUT/*.csv
    hdfs dfs -get /documentos/otc_t_calidad_altas_traficadoras/*.csv $VAL_LOCAL_RUTA_OUT
	mv $VAL_LOCAL_RUTA_OUT/*.csv $VAL_LOCAL_RUTA_OUT/$VAL_NOMBRE_ARCH_3
    sed -i 's/\banio\b/aÃ±o/g' $VAL_LOCAL_RUTA_OUT/$VAL_NOMBRE_ARCH_3

    # seteo de etapa
    echo "Procesado ETAPA 5" &>>$VAL_LOG
    # $(mysql -N <<<"update $TABLA set valor='6' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
	ETAPA="6"
fi

if [ "$ETAPA" = "6" ]; then
    VAL_SFTP_RUTA_OUT=$(echo $VAL_SFTP_RUTA_OUT | sed "s/~}</ /g")
    VAL_SFTP_RUTA_OUT=$(echo $VAL_SFTP_RUTA_OUT | tr '"' "'")
    VAL_NOM_FILE_OUT=$VAL_NOMBRE_ARCH_3
    
	VAL_MODO=0
    VAL_BANDERA_FTP=0

    sh -x $SFTP_GENERICO_SH \
        $VAL_MODO $VAL_BANDERA_FTP \
        $VAL_SFTP_USER_OUT $VAL_SFTP_PASS_OUT $VAL_SFTP_HOST_OUT $VAL_SFTP_PORT_OUT "$VAL_SFTP_RUTA_OUT" \
        $VAL_NOM_FILE_OUT $VAL_LOCAL_RUTA_OUT $VAL_LOG

    VAL_ERRORES=$(egrep 'ERROR - En la transferencia del archivo' $VAL_LOG | wc -l)

    if [ $VAL_ERRORES -ne 0 ]; then
        echo "==== ERROR en la transferencia FTP ====" >>$VAL_LOG
        exit 1
    else
        echo "==== FIN PROCESO EXPORTACION FTP ====" >>$VAL_LOG
    fi

    # seteo de etapa
    echo "Procesado ETAPA 6" &>>$VAL_LOG
    $(mysql -N <<<"update $TABLA set valor='1' where ENTIDAD = '${ENTIDAD}' and parametro = 'ETAPA';")
fi

echo "==== FIN PROCESO CALDAD ALTAS TRAFICADORAS ====" >>$VAL_LOG
