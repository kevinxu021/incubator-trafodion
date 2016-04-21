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

================
Dependencies
================

1. PDSH is required. Please add epel repo in advance or install PDSH in advance.
2. HDP 2.3 is installed 
3. Passwordless ssh to setup for root and $(whoami)
4. Add dfs.namenode.acls.enabled=true to custome hdfs-site.xml then restart HDFS
5. Place esgyn*.tar.gz file in same directory as trafodion_preSetup

================
Instructions 
================

1. sudo ./trafodion_preSetup
2. Go to Ambari GUI to add a service
3. Set number of DCS client connections
4. There can only be 1 Trafodion Master node, and X number Client nodes. A client node can not be on the same node as the master!!
5. HDFS. Zookeeper, and HBase are required to be restarted after installation for EsgynDB to connect with HBase. 
6. Restart in the following order HDFS, Zookeeper, THEN HBase HBase must be last
7. After restart go to Trafodion screen and select "initialize" from action box on right hand side
8. After this is complete, try creating a table from sqlci

