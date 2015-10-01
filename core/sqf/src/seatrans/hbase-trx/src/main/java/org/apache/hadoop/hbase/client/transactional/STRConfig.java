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

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collection;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;

import org.apache.commons.codec.binary.Hex;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.hbase.ServerName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.TableNotEnabledException;
import org.apache.hadoop.hbase.TableNotDisabledException;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos;

import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.io.compress.Compression.Algorithm;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.ipc.ServerRpcController;
import org.apache.hadoop.hbase.ipc.CoprocessorRpcChannel;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.regionserver.KeyPrefixRegionSplitPolicy;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import org.apache.hadoop.hbase.client.transactional.PeerInfo;
import org.apache.hadoop.ipc.RemoteException;

import org.apache.zookeeper.KeeperException;

import com.google.protobuf.ByteString;

/**
 * STR Config.
 */
public class STRConfig {

    static final Log LOG = LogFactory.getLog(STRConfig.class);

    static final String ZK_QUORUM = "hbase.zookeeper.quorum";
    static final String ZK_PORT   = "hbase.zookeeper.property.clientPort";

    private static boolean                     sb_replicate = false;
    private static Map<Integer, Configuration> peer_configs;
    private static Map<Integer, HConnection>   peer_connections;
    private static Map<Integer, PeerInfo>      peer_info_list;
    private static HBaseDCZK                   sv_dc_zk;
    private static String                      sv_my_cluster_id;
    private static int                         sv_peer_count = 0;

    private static STRConfig s_STRConfig = null; 

    public static void initClusterConfigs(Configuration pv_config) throws IOException {

	peer_configs = new HashMap<Integer, Configuration>();
	peer_connections = new HashMap<Integer, HConnection>();

	pv_config.set("hbase.hregion.impl", "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion");
	pv_config.setInt("hbase.client.retries.number", 3);

	String lv_my_cluster_id = System.getenv("MY_CLUSTER_ID");
	if (lv_my_cluster_id != null) {
	    if (LOG.isTraceEnabled()) LOG.trace("My cluster id: " + lv_my_cluster_id);
	    pv_config.setInt("esgyn.cluster.id", Integer.parseInt(lv_my_cluster_id));
	}
	peer_configs.put(0, pv_config);

        HConnection lv_connection = HConnectionManager.createConnection(pv_config);
	peer_connections.put(0, lv_connection);
	if (LOG.isInfoEnabled()) LOG.info("peer#0 zk quorum: " 
		 + (peer_configs.get(0)).get( ZK_QUORUM));
	if (LOG.isInfoEnabled()) LOG.info("peer#0 zk clientPort: " 
					  + (peer_configs.get(0)).get(ZK_PORT));
	
	String lv_str_replicate = System.getenv("PEERS");
	if (lv_str_replicate == null) {
	    lv_str_replicate = System.getProperty("PEERS");
	}
	if (LOG.isTraceEnabled()) LOG.trace("PEERS env var value: " + lv_str_replicate);
	String[] sv_peers;
	if (lv_str_replicate != null) {
	    sv_peers = lv_str_replicate.split(",");
	    sv_peer_count = sv_peers.length;
	    if (LOG.isTraceEnabled()) LOG.trace("sv_peers.length: " + sv_peers.length);
	    if (sv_peer_count > 0) {
		sb_replicate = true;
	    }
	
	    if (LOG.isTraceEnabled()) LOG.trace("Replicate count: " + sv_peer_count);

	    for (int i = 0; i < sv_peer_count; i++) {
		int lv_peer_num = Integer.parseInt(sv_peers[i]);
		String lv_peer_hbase_site_str = System.getenv("MY_SQROOT") + "/conf/peer" + lv_peer_num  + "/hbase-site.xml";
		if (LOG.isTraceEnabled()) LOG.trace("lv_peer_hbase_site: " + lv_peer_hbase_site_str);

		File lv_peer_file = new File(lv_peer_hbase_site_str);
		if (lv_peer_file.exists()) {
		    Path lv_config_path = new Path(lv_peer_hbase_site_str);
		    Configuration lv_config = HBaseConfiguration.create();
		    lv_config.addResource(lv_config_path);
		    if (LOG.isTraceEnabled()) LOG.trace("Putting peer info in the map for : " + lv_peer_hbase_site_str);
		    try {
			peer_configs.put(lv_peer_num,lv_config);
		    }
		    catch (Exception e) {
			LOG.error("Exception while adding peer info to the config: " + e);
		    }
		    if (LOG.isInfoEnabled()) LOG.info("peer#" 
						       + lv_peer_num 
						       + ":zk quorum: " + (peer_configs.get(lv_peer_num)).get(ZK_QUORUM));
		    if (LOG.isInfoEnabled()) LOG.info("peer#" 
						       + lv_peer_num 
						       + ":zk clientPort: " + (peer_configs.get(lv_peer_num)).get(ZK_PORT));
		    lv_connection = HConnectionManager.createConnection(lv_config);
		    peer_connections.put(lv_peer_num, lv_connection);

		}
		else {
		    if (LOG.isTraceEnabled()) LOG.trace("Peer Path does not exist: " + lv_peer_hbase_site_str);
		}
	    }
	}
    }

    private static void add_peer(Configuration pv_config,
				 int           pv_peer_num)
	throws InterruptedException, KeeperException, IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("Putting peer info in the map for cluster id: " + pv_peer_num);
	peer_configs.put(pv_peer_num, pv_config);
	
	HConnection lv_connection = HConnectionManager.createConnection(pv_config);
	peer_connections.put(pv_peer_num, lv_connection);

	if (LOG.isInfoEnabled()) LOG.info("peer#" 
					  + pv_peer_num 
					  + ":zk quorum: " + (peer_configs.get(pv_peer_num)).get(ZK_QUORUM)
					  + ":zk clientPort: " + (peer_configs.get(pv_peer_num)).get(ZK_PORT)
					  );
    }

    private static void add_peer(Configuration pv_config,
				 String pv_peer_num_string,
				 String pv_quorum,
				 String pv_port)
	throws InterruptedException, KeeperException, IOException 
    {
	Configuration lv_config = HBaseConfiguration.create(pv_config);

	lv_config.set(ZK_QUORUM, pv_quorum);
	lv_config.set(ZK_PORT, pv_port);

	int lv_peer_num = Integer.parseInt(pv_peer_num_string);
	lv_config.setInt("esgyn.cluster.id", lv_peer_num);

	add_peer(lv_config,
		 lv_peer_num);

    }

    public static void initObjects(Configuration pv_config)
	throws InterruptedException, KeeperException, IOException 
    {
	if (pv_config == null) {
	    return;
	}

	pv_config.set("hbase.hregion.impl", "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion");
	pv_config.setInt("hbase.client.retries.number", 3);

	peer_configs = new HashMap<Integer, Configuration>();
	peer_connections = new HashMap<Integer, HConnection>();
	peer_info_list = new HashMap<Integer, PeerInfo>();

	sv_dc_zk = new HBaseDCZK(pv_config);
	peer_info_list = sv_dc_zk.list_clusters();
	sv_my_cluster_id = sv_dc_zk.get_my_id();

	if (sv_my_cluster_id != null) {
	    if (LOG.isTraceEnabled()) LOG.trace("My cluster id: " + sv_my_cluster_id);
	    pv_config.setInt("esgyn.cluster.id", Integer.parseInt(sv_my_cluster_id));
	}

    }

    public static void initClusterConfigsZK(Configuration pv_config) 
	throws InterruptedException, KeeperException, IOException 
    {
	if (LOG.isTraceEnabled()) LOG.trace("initClusterConfigsZK ENTRY");

	initObjects(pv_config);

	// Put myself in the list of configurations
	add_peer(pv_config,
		 0);

	try {

	    Map<Integer, PeerInfo> lv_pi_list = sv_dc_zk.list_clusters();

	    for (PeerInfo lv_pi : lv_pi_list.values()) {
		if (LOG.isTraceEnabled()) LOG.trace("initClusterConfigsZK: " + lv_pi);

		if (lv_pi.get_id().equals(sv_my_cluster_id)) {
		    continue;
		}

		add_peer(pv_config,
			 lv_pi.get_id(),
			 lv_pi.get_quorum(),
			 lv_pi.get_port());

		sv_peer_count++;
	    }
	}
	catch (Exception e) {
	    LOG.error("Exception while adding peer info to the config: " + e);
	}
	
    }

    public String getPeerStatus(int pv_cluster_id) {
	PeerInfo lv_pi = peer_info_list.get(pv_cluster_id);
	if (lv_pi != null) {
	    return lv_pi.get_status();
	}

	return null;
    }

    public int getPeerCount() {
	return sv_peer_count;
    }

    public Configuration getPeerConfiguration(int pv_cluster_id) {
	return peer_configs.get(pv_cluster_id);
    }

    public Map<Integer, Configuration> getPeerConfigurations() {
	return peer_configs;
    }

    public HConnection getPeerConnection(int pv_peer_id) {
	return peer_connections.get(pv_peer_id);
    }

    public Map<Integer, HConnection> getPeerConnections() {
	return peer_connections;
    }

    public String getMyClusterId() {
	return sv_my_cluster_id;
    }

    // getInstance to return the singleton object for TransactionManager
    public synchronized static STRConfig getInstance(final Configuration conf) 
	throws 	IOException, InterruptedException, KeeperException, ZooKeeperConnectionException {
	if (s_STRConfig == null) {
	
	    s_STRConfig = new STRConfig(conf);
	}
	return s_STRConfig;
    }

    /**
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    private STRConfig(final Configuration conf) throws InterruptedException, KeeperException, ZooKeeperConnectionException, IOException {
	String lv_use_zk = System.getenv("STR_USE_ZK");
	if (lv_use_zk != null && 
	    (lv_use_zk.equals("0"))) {
	    initClusterConfigs(conf);
	}
	else {
	    initClusterConfigsZK(conf);
	}
    }

    public String toString() {
	StringBuilder lv_sb = new StringBuilder();
	String lv_str;
	lv_str = "Number of peers: " + sv_peer_count;
	lv_sb.append(lv_str);
	for ( Map.Entry<Integer, Configuration> e : peer_configs.entrySet() ) {
	    lv_str = "\n======\nID: " + e.getKey() + "\n";
	    lv_sb.append(lv_str);
	    lv_str = ZK_QUORUM + ": " + e.getValue().get(ZK_QUORUM);
	    lv_sb.append(lv_str);
	    lv_str = ZK_PORT + ": " + e.getValue().get(ZK_PORT);
	    lv_sb.append(lv_str);
	}

	return lv_sb.toString();
    }

    public static void main(String[] args) {
	STRConfig pSTRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	try {
	    pSTRConfig = STRConfig.getInstance(lv_config);
	}
	catch (InterruptedException int_exception) {
	    System.out.println("Interrupted Exception trying to get STRConfig instance: " + int_exception);
	}
	catch (IOException ioe) {
	    System.out.println("IO Exception trying to get STRConfig instance: " + ioe);
	}
	catch (KeeperException kpe) {
	    System.out.println("Keeper Exception trying to get STRConfig instance: " + kpe);
	}
	
	System.out.println(pSTRConfig);
    }

}

