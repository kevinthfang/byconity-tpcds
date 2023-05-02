#!/bin/bash
# 
#  Copyright (2022) Bytedance Ltd. and/or its affiliates
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  
#      http://www.apache.org/licenses/LICENSE-2.0
#  
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# 

source ./config.sh
source ./helper.sh

[ -z "$1" ] && DATASIZE=1 || DATASIZE=$1
[ -z "$2" ] && CSVPATH="$SCRIPTPATH/data_${SUITE}_${DATASIZE}" || CSVPATH=$2
IMPORT_DURATION=${IMPORT_DURATION:-logs/import_duration.txt}

DATABASE=${DATABASE:-${DB_PREFIX}${SUITE}${DATASIZE}}
EXT=dat
DELIM="|"
LAST_TABLE=web_site
DDL=$SCRIPTPATH/ddl/tpcds.sql

if [ ! -d $CSVPATH ]; then
    log "$CSVPATH directory not found."
    exit -1
fi

set -e

case $DATASIZE in
	1)	
		SORTKEY_GROUP=(ca_address_sk-1 cd_demo_sk-1 d_date_sk-1 w_warehouse_sk-1 sm_ship_mode_sk-1 t_time_sk-1 r_reason_sk-1 ib_income_band_sk-1 i_item_sk-1 s_store_sk-1 cc_call_center_sk-1 c_customer_sk-1 web_site_sk-1 sr_returned_date_sk-1 hd_demo_sk-1 wp_web_page_sk-1 p_promo_sk-1 cp_catalog_page_sk-1 inv_date_sk-1 cr_returned_date_sk-1 wr_returned_date_sk-1 ws_sold_date_sk-1 cs_sold_date_sk-1 ss_sold_date_sk-1)
		;;
	100)
		SORTKEY_GROUP=(ca_address_sk-1 cd_demo_sk-1 d_date_sk-1 w_warehouse_sk-1 sm_ship_mode_sk-1 t_time_sk-1 r_reason_sk-1 ib_income_band_sk-1 i_item_sk-1 s_store_sk-1 cc_call_center_sk-1 c_customer_sk-1 web_site_sk-1 sr_returned_date_sk-2 hd_demo_sk-1 wp_web_page_sk-1 p_promo_sk-1 cp_catalog_page_sk-1 inv_date_sk-2 cr_returned_date_sk-6 wr_returned_date_sk-4 ws_sold_date_sk-4 cs_sold_date_sk-6 ss_sold_date_sk-8)
		;;
	1000)
		SORTKEY_GROUP=(ca_address_sk-1 cd_demo_sk-1 d_date_sk-1 w_warehouse_sk-1 sm_ship_mode_sk-1 t_time_sk-1 r_reason_sk-1 ib_income_band_sk-1 i_item_sk-1 s_store_sk-1 cc_call_center_sk-1 c_customer_sk-1 web_site_sk-1 sr_returned_date_sk-22 hd_demo_sk-1 wp_web_page_sk-1 p_promo_sk-1 cp_catalog_page_sk-1 inv_date_sk-10 cr_returned_date_sk-15 wr_returned_date_sk-6 ws_sold_date_sk-100 cs_sold_date_sk-200 ss_sold_date_sk-260)
esac

SED_REPLACE=''
for SORTKEYS in ${SORTKEY_GROUP[@]}; do
	IFS=- read -r KEY BUCKET <<< "$SORTKEYS"
	eval "SED_REPLACE+='s|DISTRIBUTED BY HASH(\`${KEY}\`) BUCKETS [0-9]*|DISTRIBUTED BY HASH(\`${KEY}\`) BUCKETS ${BUCKET}|g; '"
done

sed -i "${SED_REPLACE}" $DDL 

log "Create tables for ${DATABASE}..."
sed "s|${SUITE}|${DATABASE}|g" $DDL | $EXEC 
TABLES=$($EXEC --skip-column-names -D ${DATABASE} -e 'SHOW TABLES;' | awk '{print $1}')
log "Tables created: $TABLES"

log "Import dataset from ${CSVPATH}..."
for TABLE in $TABLES; do
    CNTSQL="SELECT COUNT(*) FROM ${TABLE}"
	log "importing ${DATABASE}.${TABLE}..."

    for ORIG_F in ${CSVPATH}/${TABLE}*; do
        FNB=$(basename -- "$ORIG_F") && FILENAME="${FNB%.*}"

		if [[ ${TABLE}___ == ${FILENAME//[0-9]/} ]]; then
			TASKID=${FILENAME}-${DATASIZE}
			log "${FILENAME} - ${FNB}"
			trace "${FILENAME} - ${FNB}"
			trace "RUN: curl -u ${SRV_USER}:${SRV_PASSWORD} --location-trusted -T ${ORIG_F} -H "label:${TASKID}" -H "column_separator:${DELIM}" -H "max_filter_ratio:0.1" -H "exec_mem_limit:8589934592" http://${SRV_IP}:${SRV_HTTP_PORT}/api/${DATABASE}/${TABLE}/_stream_load"
			curl -u ${SRV_USER}:${SRV_PASSWORD} --location-trusted -T ${ORIG_F} -H "label:${TASKID}" -H "column_separator:${DELIM}" -H "max_filter_ratio:0.1" -H "exec_mem_limit:8589934592" http://${SRV_IP}:${SRV_HTTP_PORT}/api/${DATABASE}/${TABLE}/_stream_load 2>/dev/null >> $TRACE_LOG
			log "$TABLE imported count: $(${EXEC} -D $DATABASE -e "${CNTSQL}" | awk '{print $1}')"
		fi
    done
done

echo $SECONDS > $IMPORT_DURATION
log "Used ${SECONDS}s to import ${DATABASE}."

log "Checking imported count..."
case $DATASIZE in
	1)	
		ROW_CNT=(call_center-6 catalog_page-11718 catalog_returns-143974 catalog_sales-1440839 customer-100000 customer_address-50000 customer_demographics-1920800 date_dim-73049 household_demographics-7200 income_band-20 inventory-11745000 item-18000 promotion-300 reason-35 ship_mode-20 store-12 store_returns-287777 store_sales-2880029 time_dim-86400 warehouse-5 web_page-60 web_returns-71937 web_sales-720791 web_site-30)
		;;
	100)
		ROW_CNT=(call_center-30 catalog_page-20400 catalog_returns-14404374 catalog_sales-143997065 customer-2000000 customer_address-1000000 customer_demographics-1920800 date_dim-73049 household_demographics-7200 income_band-20 inventory-399330000 item-204000 promotion-1000 reason-55 ship_mode-20 store-402 store_returns-28795080 store_sales-287997024 time_dim-86400 warehouse-15 web_page-2040 web_returns-7197670 web_sales-72001237 web_site-24)
		;;
	1000)
		ROW_CNT=(call_center-42 catalog_page-30000 catalog_returns-143996756 catalog_sales-1439980416 customer-12000000 customer_address-6000000 customer_demographics-1920800 date_dim-73049 household_demographics-7200 income_band-20 inventory-783000000 item-300000 promotion-1500 reason-65 ship_mode-20 store-1002 store_returns-287999764 store_sales-2879987999 time_dim-86400 warehouse-20 web_page-3000 web_returns-71997522 web_sales-720000376 web_site-54)
esac

for CNT in ${ROW_CNT[@]}; do
	IFS=- read -r TABLE ROW <<< "${CNT}" && unset IFS
	CNTSQL="SELECT COUNT(*) FROM ${TABLE}"
	IMPORTED_COUNT=$(${EXEC} -D $DATABASE -e "${CNTSQL}" | awk '{print $1}')
	if [[ "${IMPORTED_COUNT}" != "${ROW}" ]]; then
		log "${TABLE} should have ${ROW} instead has ${IMPORTED_COUNT}"
	else
		log "${TABLE} count matches expectation."
	fi
done 