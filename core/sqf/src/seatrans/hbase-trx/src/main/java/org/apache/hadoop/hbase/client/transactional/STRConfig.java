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

import org.apache.hadoop.ipc.RemoteException;

import com.google.protobuf.ByteString;

/**
 * STR Config.
 */
public class STRConfig {

    static final Log LOG = LogFactory.getLog(STRConfig.class);

    private static boolean                     sb_replicate = false;
    private static Map<Integer, Configuration> peer_configs;
    private static Map<Integer, HConnection>   peer_connections;
    private static int                         sv_peer_count = 0;

    private static STRConfig s_STRConfig = null; 

    public static void initClusterConfigs(Configuration pv_config) throws IOException {

	peer_configs = new HashMap<Integer, Configuration>();
	peer_connections = new HashMap<Integer, HConnection>();

	pv_config.set("hbase.hregion.impl", "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion");
	pv_config.setInt("hbase.client.retries.number", 3);
	peer_configs.put(0, pv_config);

        HConnection lv_connection = HConnectionManager.createConnection(pv_config);
	peer_connections.put(0, lv_connection);
	LOG.info("peer#0 zk forum: " + (peer_configs.get(0)).get("hbase.zookeeper.quorum"));

	String lv_str_replicate = System.getenv("PEERS");
	String[] sv_peers;
	if (lv_str_replicate != null) {
	    sv_peers = lv_str_replicate.split(",");
	    sv_peer_count = sv_peers.length;
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
		    lv_config.set("hbase.hregion.impl", "org.apache.hadoop.hbase.regionserver.transactional.TransactionalRegion");
		    lv_config.setInt("hbase.client.retries.number", 3);
		    lv_config.addResource(lv_config_path);
		    if (LOG.isTraceEnabled()) LOG.trace("Putting peer info in the map for : " + lv_peer_hbase_site_str);
		    try {
			peer_configs.put(lv_peer_num,lv_config);
		    }
		    catch (Exception e) {
			LOG.error("Exception while adding peer info to the config: " + e);
		    }
		    if (LOG.isTraceEnabled()) LOG.trace("peer#" + lv_peer_num + ":zk forum: " + (peer_configs.get(lv_peer_num)).get("hbase.zookeeper.quorum"));
		    lv_connection = HConnectionManager.createConnection(lv_config);
		    peer_connections.put(lv_peer_num, lv_connection);

		}
		else {
		    if (LOG.isTraceEnabled()) LOG.trace("RMInterface static: Peer Path does not exist: " + lv_peer_hbase_site_str);
		}
	    }
	}
    }

    public int getPeerCount() {
	return sv_peer_count;
    }

    public Map<Integer, Configuration> getPeerConfigurations() {
	return peer_configs;
    }

    public Map<Integer, HConnection> getPeerConnections() {
	return peer_connections;
    }

    // getInstance to return the singleton object for TransactionManager
    public synchronized static STRConfig getInstance(final Configuration conf) throws ZooKeeperConnectionException, IOException {
	if (s_STRConfig == null) {
	
	    s_STRConfig = new STRConfig(conf);
	}
	return s_STRConfig;
    }

    /**
     * @param conf
     * @throws ZooKeeperConnectionException
     */
    private STRConfig(final Configuration conf) throws ZooKeeperConnectionException, IOException {
	initClusterConfigs(conf);
    }

}

