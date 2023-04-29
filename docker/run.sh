#!/bin/bash
# 
#  Copyright (2022) Bytedance Ltd. and/or its affiliates
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.tec
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

function run_ck() {
    docker run -d \
        -v "$(pwd)"/data:/var/lib/clickhouse/ \
        -v "$(pwd)"/logs:/var/log/clickhouse-server/ \
        -v "$(pwd)"/configd:/etc/clickhouse-server/config.d/ \
        --expose 8123 \
        --expose 9000 \
        --expose 9009 \
        --expose 9234 \
        --network host \
        --name ck-clickhouse-server --ulimit nofile=262144:262144 clickhouse/clickhouse-server:23.2.5
}

mkdir -p data/
mkdir -p logs/

run_ck
