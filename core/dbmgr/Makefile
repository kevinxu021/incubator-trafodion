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

.PHONY: all

include ../macros.gmk #top level
DBMGR_TARNAME=dbmgr-$(TRAFODION_VER).tar.gz
MGBLTY_TOOLS_TARNAME=mgblty-tools-$(TRAFODION_VER).tar.gz
CP :=/bin/cp -fp

all: dbmgr 

dbmgr:package-tools
	bower install --config.cwd=src/main/webapp --config.interactive=false
	$(MAVEN) install -DskipTests 
	-mkdir -p ../../${DISTRIBUTION_DIR}
	mv target/$(DBMGR_TARNAME) ../../${DISTRIBUTION_DIR}

install-tools:
	@echo "Installing Manageability tools required by DBMgr" 
	cd $(MY_SQROOT)/sql/scripts && install_mgblty 

package-tools: install-tools
	-mkdir -p ../../${DISTRIBUTION_DIR}
	${CP} -r scripts/bosun/* $(MY_SQROOT)/../../mgblty/bosun
	${CP} -r scripts/tcollector/* $(MY_SQROOT)/../../mgblty/tcollector
	${CP} -r scripts/opentsdb/bin/tsd.sh $(MY_SQROOT)/../../mgblty/opentsdb/bin
	${CP} -r scripts/opentsdb/bin/tsdb.local $(MY_SQROOT)/../../mgblty/opentsdb/bin
	${CP} -r scripts/opentsdb/bin/register_metrics.sh $(MY_SQROOT)/../../mgblty/opentsdb/tools
	${CP} -r scripts/opentsdb/bin/opentsdb.conf $(MY_SQROOT)/../../mgblty/opentsdb/etc/opentsdb
	tar chzf ../../${DISTRIBUTION_DIR}/$(MGBLTY_TOOLS_TARNAME) ../../mgblty 

clean:
	$(MAVEN) clean
	$(RM) -r $(MY_SQROOT)/../../mgblty
	${RM} ../../${DISTRIBUTION_DIR}/$(DBMGR_TARNAME)
	${RM} ../../${DISTRIBUTION_DIR}/$(MGBLTY_TOOLS_TARNAME)

cleanall: clean
	$(RM) -r $(MY_SQROOT)/../dbmgr/mgblty_download
