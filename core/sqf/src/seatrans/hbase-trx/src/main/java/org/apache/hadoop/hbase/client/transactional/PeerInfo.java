// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.apache.hadoop.hbase.client.transactional;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Multi Data Center specific
 */
public class PeerInfo {

    static final Log LOG = LogFactory.getLog(PeerInfo.class);

    public static final String TRAFODION_UP             = "tup";
    public static final String TRAFODION_DOWN           = "tdn";
    public static final String HBASE_UP                 = "hup";
    public static final String HBASE_DOWN               = "hdn";
    public static final String STR_UP                   = "sup";
    public static final String STR_DOWN                 = "sdn";

    private String m_id;
    private String m_quorum;
    private String m_port;
    private String m_status;

    public PeerInfo() {
	if (LOG.isTraceEnabled()) LOG.trace("PeerInfo (s,s,s,s) -- ENTRY");

	m_id = null;
	m_quorum = null;
	m_port = null;
	m_status = null;

    }

    public PeerInfo(String p_id, 
		    String p_quorum, 
		    String p_port, 
		    String p_status) 
    {
	if (LOG.isTraceEnabled()) LOG.trace("PeerInfo (s,s,s,s) -- ENTRY");

	m_id = p_id;
	m_quorum = p_quorum;
	m_port = p_port;
	m_status = p_status;
    }

    public String get_id() {
	return m_id;
    }

    public void set_id(String p_id) {
	m_id = p_id;
    }

    public String get_quorum() {
	return m_quorum;
    }

    public void set_quorum(String p_quorum) {
	m_quorum = p_quorum;
    }

    public void set_quorum(byte[] p_quorum) {
	if (p_quorum != null) {
	    m_quorum = new String(p_quorum);
	}
    }

    public String get_port() {
	return m_port;
    }

    public void set_port(String p_port) {
	m_port = p_port;
    }

    public void set_port(byte[] p_port) {
	if (p_port != null) {
	    m_port = new String(p_port);
	}
    }

    public String get_status() {
	return m_status;
    }

    public void set_status(String p_status) {
	m_status = p_status;
    }
	
    public void set_status(byte[] p_status) {
	if (p_status != null) {
	    m_status = new String(p_status);
	}
    }

    public String toString()  
    {
	StringBuilder lv_string = new StringBuilder();
	lv_string = lv_string
	    .append(m_id)
	    .append(":")
	    .append(m_quorum)
	    .append(":")
	    .append(m_port)
	    .append(":")
	    .append(m_status)
	    ;

	return lv_string.toString();
    }

    public static void main(String [] Args) throws Exception 
    {
	PeerInfo lv_peer = new PeerInfo("1", "q", "24000", "Ready");
	System.exit(0);
    }

}
