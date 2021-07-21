#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

while echo "Running"; do
    # V1
    cmd="airflow scheduler -n 5"
    if ! $cmd ; then
        echo "Scheduler crashed. Respawning.." >&2
        date >> /tmp/airflow_scheduler_errors.txt
        sleep 1
    fi

    # V2
    #eval "${cmd}"
    #return_code=$?
    #if (( return_code != 0 )); then
    #    echo "Scheduler crashed with exit code $return_code. Respawning.." >&2
    #    date >> /tmp/airflow_scheduler_errors.txt
    #    sleep 1
    #fi

done
