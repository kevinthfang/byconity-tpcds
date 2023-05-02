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
DATABASE=${DATABASE:-${DB_PREFIX}${SUITE}${DATASIZE}${DB_SUFFIX}}

TIMEOUT=${TIMEOUT:-600}
RESULT=${RESULT:-${LOGDIR}/result.csv}
SQL_DIR=${SCRIPTPATH}/sql
LOG_CUR=$LOGDIR/curr.txt
TIME_FILE=$LOGDIR/time.txt

echo "qid,duration,status" > "$RESULT"

set -e

function measure_time() {
	/usr/bin/time -p -o ${TIME_FILE} $@
}

function parse_time() {
	sec_to_ms $(cat ${TIME_FILE} | grep real | awk '{print $2}'; rm ${TIME_FILE} > /dev/null)
}

echo "warm up..."
if [ -f SQL_DIR/Warmup.sql ]; then
	cat SQL_DIR/Warmup.sql | $EXEC -D ${DATABASE} > /dev/null
fi

QPATH="${SQL_DIR}/doris"

# SKIPLIST=()
for FILE in ${QPATH}/*.sql; do
	QUERY=$(query_file_to_id $FILE)

	for skip in "${SKIPLIST[@]}"; do
		if [ "$QUERY" -eq "$skip" ]; then
			echo "[+Q]$(echo $i) skipped"
			continue 2
		fi
	done

    # sed -re "s/${SUITE}\./${DATABASE}\./g" $FILE | $EXEC -D ${DATABASE}

	sed -re "s/${SUITE}\./${DATABASE}\./g" $FILE | measure_time timeout $TIMEOUT $EXEC -D ${DATABASE} > ${LOG_CUR} 2>&1 && RET=0 || RET=$?

	DURATION=$(parse_time)

    log "[Query$QID]duration: ${DURATION}ms, status: ${RET}"
	echo "${QID},${DURATION},${RET}" >> $RESULT
done

