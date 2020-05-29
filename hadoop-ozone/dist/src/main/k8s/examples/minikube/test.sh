#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

export K8S_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"

# shellcheck source=/dev/null
source "$K8S_DIR/../testlib.sh"

OZONE_ROOT=$(realpath "$K8S_DIR/../../../../../target/ozone-0.6.0-SNAPSHOT")

flekszible generate -t ozone/devbuild:path=$OZONE_ROOT

start_k8s_env "$K8S_DIR"

execute_robot_test om auditparser

execute_robot_test scm basic/basic.robot

stop_k8s_env "$K8S_DIR"
