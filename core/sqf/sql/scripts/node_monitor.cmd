#!/bin/bash
# @@@ START COPYRIGHT @@@
#
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
#
# @@@ END COPYRIGHT @@@
#
# Add code to monitor processes/services at a node level
#
# The stdout is the file $MY_SQROOT/sql/scripts/stdout_nmon
#
#---- Begin: Setup the env to run any script - please do not edit this block
cd $MY_SQROOT
. $MY_SQROOT/sqenv.sh
cd - >/dev/null

# Setting this variable so that downstream scripts executed here are aware of the context
export NODE_MONITOR_MODE=1
#----  End : Setup the env to run any script - please do not edit this block

$MY_SQROOT/mgblty/opentsdb/bin/tsd.sh watch 

$MY_SQROOT/mgblty/tcollector/startstop watch 
