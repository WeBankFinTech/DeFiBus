#!/bin/sh

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if [ ! -f "pid.file" ]; then
        Result_pid="noPid"
else
        Result_pid=`cat pid.file`
fi


Result=$(ps -ef|grep DeFiBusBrokerStartup|grep -v grep|grep $Result_pid)
if [ "" == "$Result" ]
then
	export LANG=zh_CN.utf8
	export LC_ALL=zh_CN.UTF-8
	export LC_CTYPE=en_US.UTF-8
	./runbroker.sh
fi
