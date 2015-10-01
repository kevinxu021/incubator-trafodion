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

import java.io.IOException;

import java.nio.charset.Charset;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.Abortable;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;

/**
 * Multi Data Center specific Zookeeper operations
 */
public class HBaseDCZK implements Abortable {

    static final Log LOG = LogFactory.getLog(HBaseDCZK.class);

    private static final Charset CHARSET        = Charset.forName("UTF-8");

    public static final String m_my_cluster_id_string = "my_cluster_id";
    public static final String m_clusters_string      = "clusters";
    public static final String m_quorum_string        = "quorum";
    public static final String m_port_string          = "port";
    public static final String m_status_string        = "status";

    public static final String m_base_node            = "/hbase/Trafodion/multi_dc";
    public static final String m_clusters_node        = m_base_node + "/" + m_clusters_string;
    public static final String m_my_cluster_id_node   = m_base_node + "/" + m_my_cluster_id_string;

    ZooKeeperWatcher m_zkw;
    Configuration    m_config;
    Set<String>      nodeList;	
	
    /**
     * @param conf
     * @throws Exception
     */
    public HBaseDCZK(final Configuration pv_config) throws InterruptedException, KeeperException, IOException {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK(conf) -- ENTRY");
	m_config = pv_config;
	this.m_zkw = new ZooKeeperWatcher(m_config, "DC", this, false);
    }
	
    /**
     * @param znode
     * @return znode data
     * @throws KeeperException
     */
    private byte [] get_znode_data (String znode) throws KeeperException, InterruptedException {
	return ZKUtil.getData(m_zkw, znode);
    }
	
    /**
     * @param znode
     * @return List<String> of children nodes
     * @throws KeeperException
     */
    private List<String> get_znode_children(String pv_node) throws KeeperException {
	return ZKUtil.listChildrenNoWatch(m_zkw, pv_node);
    }

    /**
     * @param toDelete
     * @throws KeeperException
     */
    public void delete_cluster_entry(String pv_cluster_id ) throws KeeperException {
	LOG.info("delete_cluster_entry -- ENTRY -- key: " + pv_cluster_id);
	ZKUtil.deleteNodeFailSilent(m_zkw, m_clusters_node + "/" + pv_cluster_id);
	LOG.info("delete_cluster_entry -- EXIT ");
    }

    /**
     * @param cluster Id
     * @param quorum
     * @param port
     * @param status
     * @throws IOException
     */
    public void set_peer_znode(String pv_cluster_id,
			       String pv_quorum, 
			       String pv_port,
			       String pv_status
			       ) throws IOException {

	LOG.info("HBaseDCZK:set_peer_znode: "
		 + " cluster_id : " + pv_cluster_id
		 + " quorum: " + pv_quorum
		 + " port: " + pv_port
		 + " status: " + pv_status
		 );
		 
	try {
	    String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;
	    ZKUtil.createWithParents(m_zkw, lv_cluster_id_node);
	    
	    if ((pv_quorum != null) && (pv_quorum.length() > 0)) {
		String lv_quorum_node = lv_cluster_id_node + "/" + m_quorum_string;
		ZKUtil.createSetData(m_zkw, lv_quorum_node, pv_quorum.getBytes(CHARSET));
	    }

	    if ((pv_port != null) && (pv_port.length() > 0)) {
		String lv_port_node = lv_cluster_id_node + "/" + m_port_string;
		ZKUtil.createSetData(m_zkw, lv_port_node, pv_port.getBytes(CHARSET));
	    }

	    if ((pv_status != null) && (pv_status.length() > 0)) {
		String lv_status_node = lv_cluster_id_node + "/" + m_status_string;
		ZKUtil.createSetData(m_zkw, lv_status_node, pv_status.getBytes(CHARSET));
	    }

	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:set_peer_znode: ZKW Unable to create peer zNode: " 
				  + m_clusters_node + "/" + pv_cluster_id
				  + " , throwing IOException " + e);
	}
    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public PeerInfo get_peer_znode(String pv_cluster_id) throws IOException {

	LOG.info("HBaseDCZK:get_peer_znode: "
		 + " cluster_id : " + pv_cluster_id
		 );

	String lv_cluster_id_node = m_clusters_node + "/" + pv_cluster_id;
	try {
	    PeerInfo lv_pi = new PeerInfo();
	    lv_pi.set_id(pv_cluster_id);

	    String lv_quorum_node = lv_cluster_id_node + "/" + m_quorum_string;
	    lv_pi.set_quorum(get_znode_data(lv_quorum_node));

	    String lv_port_node = lv_cluster_id_node + "/" + m_port_string;
	    lv_pi.set_port(get_znode_data(lv_port_node));

	    String lv_status_node = lv_cluster_id_node + "/" + m_status_string;
	    lv_pi.set_status(get_znode_data(lv_status_node));

	    return lv_pi;

	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:get_peer_znode: ZKW Unable to get peer zNode: " 
				  + lv_cluster_id_node
				  + " , throwing IOException " + e);
	} catch (InterruptedException e) {
	    throw new IOException("HBaseDCZK:get_peer_znode: ZKW Unable to get peer zNode: " 
				  + lv_cluster_id_node
				  + " , throwing IOException " + e);
	}

    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public boolean delete_peer_znode(String pv_cluster_id) throws IOException {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK:delete_peer_znode: "
					    + " cluster_id : " + pv_cluster_id
					    );

	try {
	    String lv_myid = get_my_id();
	    
	    if (pv_cluster_id.equals(lv_myid)) {
		return false;
	    }
	    
	    ZKUtil.deleteNodeRecursively(m_zkw, m_clusters_node + "/" + pv_cluster_id);

	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:delete_peer_znode: ZKW, KeeperException trying to delete: " 
				  + pv_cluster_id
				  + " , throwing IOException " + e);
	} catch (InterruptedException e) {
	    throw new IOException("HBaseDCZK:delete_peer_znode: ZKW, InterruptedException trying to delete: " 
				  + pv_cluster_id
				  + " , throwing IOException " + e);
	}

	return true;

    }

    /**
     * @param cluster Id
     * @throws IOException
     */
    public Map<Integer, PeerInfo> list_clusters() throws KeeperException, IOException {
	LOG.info("HBaseDCZK:list_clusters"
		 );

	List<String> peer_node_strings         = new ArrayList<String>();
	Map<Integer, PeerInfo> peer_info_list  = new HashMap<Integer, PeerInfo>();
	
	peer_node_strings = get_znode_children(m_clusters_node);
	if (peer_node_strings == null) {
	    return null;
	}

	for (String peer_node_string : peer_node_strings) {
	    int lv_id = Integer.parseInt(peer_node_string);
	    PeerInfo lv_pi = get_peer_znode(peer_node_string);
	    peer_info_list.put(lv_id, lv_pi);
	}

	return peer_info_list;
    }

    /**
     * @param cluster Id
     * sync ZK information from this cluster to the provided cluster
     * @throws IOException
     */
    public void sync_cluster(String pv_cluster_id) throws KeeperException, IOException {
	LOG.info("HBaseDCZK:sync_cluster"
		 );

	Map<Integer, PeerInfo> lv_pi_list = list_clusters();

	if (lv_pi_list == null) {
	    return;
	}

	try {
	    
	    STRConfig     pSTRConfig = null;
	    Configuration lv_config  = null;
	    System.setProperty("PEERS", pv_cluster_id);
	    pSTRConfig = STRConfig.getInstance(m_config);
	    lv_config = pSTRConfig.getPeerConfiguration(Integer.parseInt(pv_cluster_id));
	    if (lv_config == null) {
		System.out.println("Peer ID: " + pv_cluster_id + " does not exist OR it has not been configured.");
		return;
	    }
		
	    HBaseDCZK lv_zk = new HBaseDCZK(lv_config);
	    
	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		lv_zk.set_peer_znode(lv_pi.get_id(),
				     lv_pi.get_quorum(), 
				     lv_pi.get_port(),
				     lv_pi.get_status());
	    }
	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:sync_cluster: ZKW, KeeperException trying to get my id: " 
				  + " , throwing IOException " + e);
	} catch (InterruptedException e) {
	    throw new IOException("HBaseDCZK:sync_cluster: ZKW, InterruptedException trying to get my id: " 
				  + " , throwing IOException " + e);
	} catch (Exception e) {
	    System.out.println("exception: " + e);
	    System.exit(1);
	}

    }

    public void sync_clusters() throws KeeperException, IOException {
	LOG.info("HBaseDCZK:sync_clusters"
		 );

	Map<Integer, PeerInfo> lv_pi_list = list_clusters();
	
	if (lv_pi_list == null) {
	    return;
	}

	try {
	    
	    String        lv_my_id   = get_my_id();
	    
	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		if (lv_pi.get_id().equals(lv_my_id)) {
		    continue;
		}
		
		sync_cluster(lv_pi.get_id());
	    }
	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:sync_clusters: ZKW, KeeperException trying to get my id: " 
				  + " , throwing IOException " + e);
	} catch (InterruptedException e) {
	    throw new IOException("HBaseDCZK:sync_clusters: ZKW, InterruptedException trying to get my id: " 
				  + " , throwing IOException " + e);
	}

    }

    /**
     * @param my_cluster_id
     * @throws IOException
     */
    public void set_my_id(String pv_my_cluster_id) throws IOException {
	LOG.info("HBaseDCZK:set_my_id: cluster ID: " + pv_my_cluster_id);

	try {
	    ZKUtil.createSetData(m_zkw, m_my_cluster_id_node, pv_my_cluster_id.getBytes(CHARSET));
	} catch (KeeperException e) {
	    throw new IOException("HBaseDCZK:set_my_id: ZKW Unable to create zNode: " 
				  + pv_my_cluster_id
				  + " , throwing IOException " + e);
	}
    }

    /**
     * @return
     */
    public String get_my_id() throws KeeperException, InterruptedException {
	if (LOG.isTraceEnabled()) LOG.trace("HBaseDCZK:get_my_id");

	byte[] b_my_cluster_id = get_znode_data(m_my_cluster_id_node);
	if (b_my_cluster_id != null) {
	    String lv_cid = new String(b_my_cluster_id);
	    if (LOG.isTraceEnabled()) LOG.trace("get_my_id id: " + lv_cid);
	    return lv_cid;
	}

	return null;
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.Abortable#abort(java.lang.String, java.lang.Throwable)
     */
    @Override
	public void abort(String arg0, Throwable arg1) {
	// TODO Auto-generated method stub
		
    }

    /* (non-Javadoc)
     * @see org.apache.hadoop.hbase.Abortable#isAborted()
     */
    @Override
	public boolean isAborted() {
	// TODO Auto-generated method stub
	return false;
    }

    static void usage() {
	
	System.out.println("usage:");
	System.out.println("HBaseDCZK [<command> | <options>...]");
	System.out.println("<command>         : <   -setmyid <id>");
	System.out.println("                  :   | -getmyid");
	System.out.println("                  :   | -set <cluster info>");
	System.out.println("                  :   | -get <id>");
	System.out.println("                  :   | -list");
	System.out.println("                  :   | -delete <id>");
	System.out.println("                  :   | -sync >");
	System.out.println("<options>         : [ <peer info> | -h | -v ]");
	System.out.println("<cluster info>    : < <cluster id> [ <quorum info> | <port info> | <status info> ]... >");
	System.out.println("<cluster id>      : -id <id> ");
	System.out.println("<quorum info>     : -quorum <zookeeper quorum>");
	System.out.println("<port info>       : -port <zookeeper client port>");
	System.out.println("<status info>     : -status <status>");
	System.out.println("<status>          : <HBase Status>:<Trafodion Status>:<STR Status>");
	System.out.println("<HBase Status>    : <HBase Up>    (hup)|<HBase Down>     (hdn)");
	System.out.println("<Trafodion Status>: <Trafodion Up>(tup)|<Trafodion Down> (tdn)");
	System.out.println("<STR Status>      : <STR Up>      (sup)|<HBase Down>     (sdn)");
	System.out.println("<peer info>       : -peer <id>");
	System.out.println("                  :    Execute the command at the specified peer.");
	System.out.println("                  :    (Defaults to the local cluster)");
	System.out.println("<id>              : A number between 1 and 100 (inclusive)");
	System.out.println("-h                : Help (this output).");
	System.out.println("-v                : Verbose output. ");

    }

    public static void main(String [] Args) throws Exception {

	boolean lv_retcode = true;
	boolean lv_verbose = false;

	int     lv_peer_id = 0;

	String  lv_my_id  = new String();
	String  lv_id     = new String();
	String  lv_quorum = new String();
	String  lv_port   = new String();
	String  lv_status = new String();

	boolean lv_cmd_set_my_id  = false;
	boolean lv_cmd_get_my_id  = false;
	boolean lv_cmd_set        = false;
	boolean lv_cmd_get        = false;
	boolean lv_cmd_delete     = false;
	boolean lv_cmd_list       = false;
	boolean lv_cmd_sync       = false;
	boolean lv_cmd_liststatus = false;

	int lv_index = 0;
	for (String lv_arg : Args) {
	    lv_index++;
	    if (lv_arg.compareTo("-h") == 0) {
		usage();
		System.exit(0);
	    }
	    if (lv_arg.compareTo("-v") == 0) {
		lv_verbose = true;
	    }
	    else if (lv_arg.compareTo("-peer") == 0) {
		lv_peer_id = Integer.parseInt(Args[lv_index]);
		if (lv_verbose) System.out.println("Talk to Peer Cluster, ID: " + lv_peer_id);
	    }
	    else if (lv_arg.compareTo("-id") == 0) {
		lv_id =Args[lv_index];
		int lv_integer_id = Integer.parseInt(lv_id);
		if ((lv_integer_id < 1) || (lv_integer_id > 100)) {
		    System.out.println("The id has to be between 1 and 100 (inclusive). Exitting...");
		    usage();
		    System.exit(1);
		}
		if (lv_verbose) System.out.println("Cluster ID: " + lv_id);
	    }
	    else if (lv_arg.compareTo("-status") == 0) {
		lv_status =Args[lv_index];
		if (lv_verbose) System.out.println("Status: " + lv_status);
	    }
	    else if (lv_arg.compareTo("-quorum") == 0) {
		lv_quorum =Args[lv_index];
		if (lv_verbose) System.out.println("Quorum: " + lv_quorum);
	    }
	    else if (lv_arg.compareTo("-port") == 0) {
		lv_port =Args[lv_index];
		if (lv_verbose) System.out.println("Port: " + lv_port);
	    }
	    else if (lv_arg.compareTo("-setmyid") == 0) {
		if (lv_verbose) System.out.print("Command: setmyid:");
		lv_my_id =Args[lv_index];
		if ((lv_my_id != null) && (lv_my_id.length() > 0)) {
		    if (lv_verbose) System.out.println(lv_my_id);
		    int lv_integer_id = Integer.parseInt(lv_my_id);
		    if ((lv_integer_id < 1) || (lv_integer_id > 100)) {
			System.out.println("The id has to be between 1 and 100 (inclusive). Exitting...");
			usage();
			System.exit(1);
		    }
		    lv_cmd_set_my_id = true;
		}
		else {
		    System.out.println("Id not provided. Exitting...");
		    System.exit(1);
		}
	    }
	    else if (lv_arg.compareTo("-getmyid") == 0) {
		if (lv_verbose) System.out.println("Command: getmyid");
		lv_cmd_get_my_id = true;
	    }
	    else if (lv_arg.compareTo("-set") == 0) {
		if (lv_verbose) System.out.println("Command: set");
		lv_cmd_set = true;
	    }
	    else if (lv_arg.compareTo("-get") == 0) {
		if (lv_verbose) System.out.println("Command: get");
		lv_id =Args[lv_index];
		if ((lv_id != null) && (lv_id.length() > 0)) {
		    lv_cmd_get = true;
		}
		else {
		    System.out.println("Id not provided. Exitting...");
		    System.exit(1);
		}
	    }
	    else if (lv_arg.compareTo("-delete") == 0) {
		if (lv_verbose) System.out.println("Command: delete");
		lv_id =Args[lv_index];
		if ((lv_id != null) && (lv_id.length() > 0)) {
		    lv_cmd_delete = true;
		}
		else {
		    System.out.println("Id not provided. Exitting...");
		    System.exit(1);
		}
	    }
	    else if (lv_arg.compareTo("-liststatus") == 0) {
		if (lv_verbose) System.out.println("Command: list");
		lv_cmd_liststatus = true;
	    }
	    else if (lv_arg.compareTo("-list") == 0) {
		if (lv_verbose) System.out.println("Command: list");
		lv_cmd_list = true;
	    }
	    else if (lv_arg.compareTo("-sync") == 0) {
		if (lv_verbose) System.out.println("Command: sync");
		lv_cmd_sync = true;
	    }
	}
	    
	STRConfig pSTRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	if (lv_peer_id > 0) {
	    try {
		System.setProperty("PEERS", String.valueOf(lv_peer_id));
		pSTRConfig = STRConfig.getInstance(lv_config);
		lv_config = pSTRConfig.getPeerConfiguration(lv_peer_id);
		if (lv_config == null) {
		    System.out.println("Peer ID: " + lv_peer_id + " does not exist OR it has not been configured.");
		    System.exit(1);
		}
	    }
	    catch (ZooKeeperConnectionException zke) {
		System.out.println("Zookeeper Connection Exception trying to get STRConfig instance: " + zke);
		System.exit(1);
	    }
	    catch (IOException ioe) {
		System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
		System.exit(1);
	    }
	}

	try {
	    HBaseDCZK lv_zk = new HBaseDCZK(lv_config);
	    if (lv_cmd_set_my_id) {
		lv_zk.set_my_id(lv_my_id);
	    }
	    else if (lv_cmd_get_my_id) {
		System.out.println(lv_zk.get_my_id());
	    }
	    else if (lv_cmd_set) {
		if ((lv_status == null) || (lv_status.length() == 0)) {
		    lv_status = PeerInfo.STR_UP;
		}
		lv_zk.set_peer_znode(lv_id, lv_quorum, lv_port, lv_status);
	    }
	    else if (lv_cmd_get) {
		if (lv_verbose) System.out.println(lv_id);
		PeerInfo lv_pi = lv_zk.get_peer_znode(lv_id);
		if (lv_pi != null) {
		    System.out.println(lv_pi);
		}
	    }
	    else if (lv_cmd_delete) {
		if (lv_verbose) System.out.println(lv_id);
		lv_retcode = lv_zk.delete_peer_znode(lv_id);
		if (! lv_retcode) {
		    System.out.println("Error while deleting the peer znode: " + lv_id);
		}
		else {
		    System.out.println("Successfully deleted the peer znode: " + lv_id);
		}		    
	    }
	    else if (lv_cmd_list) {
		Map<Integer, PeerInfo> lv_pi_list = lv_zk.list_clusters();
		if (lv_pi_list != null) {
		    for (PeerInfo lv_pi : lv_pi_list.values()) {
			System.out.println(lv_pi);
		    }
		}
	    }
	    else if (lv_cmd_sync) {
		lv_zk.sync_clusters();
	    }
	    else if (lv_cmd_liststatus) {
		Map<Integer, PeerInfo> lv_pi_list = lv_zk.list_clusters();
		if (lv_pi_list != null) {
		    for (PeerInfo lv_pi : lv_pi_list.values()) {
			System.out.println(lv_pi.get_id() + ":" + lv_pi.get_status());
		    }
		}
	    }
	    else {
		usage();
	    }
	}
	catch (Exception e)
	    {
		System.out.println("exception: " + e);
		System.exit(1);
	    }

	if (! lv_retcode) {
	    System.exit(1);
	}

	System.exit(0);
    }
}
