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

SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
LOGDIR=${LOGDIR:-${SCRIPTPATH}/logs}
OUTPUT_LOG=${LOGDIR}/output.log
TRACE_LOG=${LOGDIR}/trace.log
TOOLS_PATH=${SCRIPTPATH}/build/tools
SUITE=${SUITE:-tpcds}
CLUSTER_NAME=${CLUSTER_NAME:-cluster}
ENABLE_TRACE="true"

export EXEC="$MYSQL_ROOT/bin/mysql -rsq --host ${SRV_IP} --port ${SRV_TCP_PORT} --user ${SRV_USER}"

if [ ! -d $LOGDIR ]; then
    mkdir -p $LOGDIR
fi

function log() {
    echo "[$(date +"%x %T.%3N")] $1" | tee -a $OUTPUT_LOG
}

function trace() {
    echo "[$(date +"%x %T.%3N")] $1" >> $TRACE_LOG
}

function query_file_to_id() {
	echo $(basename -s .sql $1) | sed 's/^0\?//g'
}

function sec_to_ms() {
	local DURATION=$(bc <<< $1*1000)
	echo ${DURATION%.*}
}
