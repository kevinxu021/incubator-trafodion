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

package org.trafodion.dtm;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

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
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;

import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.transactional.TransactionRegionLocation;
import org.apache.hadoop.hbase.client.transactional.STRConfig;

import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.KeeperException;

/**
 * Zookeeper client-side communication class for the DTM
 */
public class HBaseTmZK implements Abortable{
	public static final String rootNode = "/hbase/Trafodion/recovery";
	private final String baseNode = "/hbase/Trafodion/recovery/TM";
    private final String zNodeLDTMPath = "/hbase/Trafodion/recovery/TM/LDTM";
    private final String zNodeTLOGPath = "/hbase/Trafodion/recovery/TM/TLOG";
	static final Log LOG = LogFactory.getLog(HBaseTmZK.class);
	
	ZooKeeperWatcher zooKeeper;
	Set<String>     nodeList;	
	String zkNode;
	short dtmID;
	
	/**
	 * @param conf
	 * @throws IOException
	 */
	public HBaseTmZK(final Configuration conf) throws IOException {
                if (LOG.isTraceEnabled()) LOG.trace("HBaseTmZK(conf) -- ENTRY");
		this.dtmID = 0;
		this.zkNode = baseNode + "0";
		this.zooKeeper = new ZooKeeperWatcher(conf, "TM Recovery", this, true);
	}
	
	/**
	 * @param conf
	 * @param dtmID
	 * @throws IOException
	 */
	public HBaseTmZK(final Configuration conf, final short dtmID) throws IOException {
        if (LOG.isTraceEnabled()) LOG.trace("HBaseTmZK(conf, dtmID) -- ENTRY");
		this.dtmID = dtmID;
		this.zkNode = baseNode + String.format("%d", dtmID);
        if (LOG.isTraceEnabled()) LOG.trace("HBaseTmZK(conf, dtmID) -- zkNode" + this.zkNode);
		this.zooKeeper = new ZooKeeperWatcher(conf, "TM Recovery", this, true);
	}

	/**
	 * @param znode
	 * @return
	 * @throws KeeperException
	 */
	private byte [] checkData (String znode) throws KeeperException, InterruptedException {
		return ZKUtil.getData(zooKeeper, znode);
	}
	
	/**
	 * @return
	 */
	public short getTMID() {
		return dtmID;
	}
	
	
	/**
	 * @return
 	 * @throws KeeperException
	 */
	private List<String> getChildren() throws KeeperException {
		return ZKUtil.listChildrenNoWatch(zooKeeper, zkNode);
	}

    public void printChildren(String pv_node, int pv_depth) throws KeeperException, InterruptedException {

	if (pv_depth == 0) {System.out.println(pv_node);};
	pv_depth++;

	List<String> lv_child_list = ZKUtil.listChildrenNoWatch(zooKeeper, pv_node);
	java.util.Collections.sort(lv_child_list);
	for (String lv_child : lv_child_list ) {
	    for (int i = 0; i < pv_depth; i++) System.out.print(" ");
	    System.out.print(lv_child);
	    String lv_child_node = pv_node + "/" + lv_child;
	    byte[] lv_child_data = checkData(lv_child_node);
	    System.out.println("[" + new String(lv_child_data) + "]");
	    printChildren(lv_child_node, pv_depth);
	}
    }

	/**
	 * @return
	 * @throws KeeperException
	 */
	public Map<String,byte []> checkForRecovery() throws InterruptedException, KeeperException {
                // if (LOG.isTraceEnabled()) LOG.trace("checkForRecovery -- ENTRY");
		if(ZKUtil.nodeHasChildren(zooKeeper, zkNode)) {
			List<String> nodeChildren = new ArrayList<String>();
			Map<String, byte []> nodeDataMap = new HashMap<String, byte []>();
			nodeChildren = getChildren();

			for(String node : nodeChildren) {

                        if (LOG.isTraceEnabled()) LOG.trace("checkForRecovery -- found node: '" + node + "'");
                        byte [] nodeData = checkData(zkNode +"/" + node);
                        if (LOG.isTraceEnabled()) LOG.trace("checkForRecovery -- found node: " + node + " node data " + nodeData.toString());
				nodeDataMap.put(node, nodeData);
			}
                        if (LOG.isTraceEnabled()) LOG.trace("checkForRecovery -- EXIT returning " + nodeDataMap.size() + " regions");
                                return nodeDataMap;
                }
                else {
                        // if (LOG.isTraceEnabled()) LOG.trace(zkNode + " is currently not present.");
                        // if (LOG.isTraceEnabled()) LOG.trace("checkForRecovery -- EXIT -- node not present");
                        return null;
                }
	}

	/**
	 * @param toDelete
	 * @throws KeeperException
	 */
	public void deleteRegionEntry(Map.Entry<String,byte[]> toDelete ) throws KeeperException {
           LOG.info("deleteRegionEntry -- ENTRY -- key: " + toDelete.getKey() + " value: " + new String(toDelete.getValue()));
           ZKUtil.deleteNodeFailSilent(zooKeeper, zkNode + "/" + toDelete.getKey());
           LOG.info("deleteRegionEntry -- EXIT ");
        }

	/**
	 * @param node
	 * @param hostName
	 * @param portNumber
	 * @param encodedName
	 * @param data
	 * @throws IOException
	 */
        public void createRecoveryzNode(String hostName, int portNumber, String encodedName, byte [] data) throws IOException {
           LOG.info("HBaseTmZK:createRecoveryzNode: hostName: " + hostName + " port: " + portNumber +
                     " encodedName: " + encodedName + " data: " + new String(data));
           // default zNodePath for recovery
           String zNodeKey = hostName + "," + portNumber + "," + encodedName;

           LOG.info("HBaseTmZK:createRecoveryzNode: ZKW Post region recovery znode" + this.dtmID + " zNode Path " + zkNode);
           // create zookeeper recovery zNode, call ZK ...
           try {
              if (ZKUtil.checkExists(zooKeeper, zkNode) == -1) {
                 // create parent nodename
                 LOG.info("HBaseTmZK:createRecoveryzNode:: ZKW create parent zNodes " + zkNode);
                 ZKUtil.createWithParents(zooKeeper, zkNode);
              }
              ZKUtil.createAndFailSilent(zooKeeper, zkNode + "/" + zNodeKey, data);
           } catch (KeeperException e) {
              throw new IOException("HBaseTmZK:createRecoveryzNode: ZKW Unable to create recovery zNode: " + zkNode + " , throwing IOException ", e);
           }
        }
        /**
         ** @param data
         ** #throws IOException
         **/
        public void createGCzNode(byte [] data) throws IOException {
            String zNodeGCPath = "/hbase/Trafodion/GC";
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeGCPath) == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion table data clean up no znode path created " + zNodeGCPath);
                    ZKUtil.createWithParents(zooKeeper, zNodeGCPath);
                }
                String zNodeKey = dtmID+"";
                ZKUtil.createSetData(zooKeeper, zNodeGCPath + "/" + zNodeKey, data);
            } catch (KeeperException e) {
                throw new IOException("HBaseTmZK:createGCzNode: ZKW Unable to create GC zNode: " + zNodeGCPath +"  , throwing IOException ", e);
            }
        }
        
        /**
         ** @param data
         ** #throws IOException
         **/
        public void createLDTMezNode(byte [] data) throws IOException {
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeLDTMPath) == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion table data clean up no znode path created " + zNodeLDTMPath);
                    ZKUtil.createWithParents(zooKeeper, zNodeLDTMPath);
                }
                ZKUtil.createEphemeralNodeAndWatch(zooKeeper, zNodeLDTMPath + "/EZ", data);
            } catch (KeeperException e) {
                throw new IOException("HBasePeerTmZK:createGCzNode: ZKW Unable to create LDTM zNode: " + zNodeLDTMPath + "  , throwing IOException " + e);
            }
        }

        /**
         ** @param data
         ** #throws IOException
         **/
        public boolean isLDTMezNodeCreated() throws IOException {
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeLDTMPath+"/EZ") == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion table data clean up no znode path created " + zNodeLDTMPath);
                   LOG.info("LDTM is not ready");
                   return false;
                }
            } catch (KeeperException e) {
                throw new IOException("HBasePeerTmZK:createGCzNode: ZKW Unable to check LDTM zNode: " + zNodeLDTMPath + "  , throwing IOException " + e);
            }
            LOG.info("LDTM is ready");
            return true;
        }

        /**
         ** @param dataString zNodeKey = basePath + "/LDTM;
         ** #throws IOException
         **/
        public void deleteTLOGSyncNode() throws IOException {
            String zNodeTLOGPathKey = zNodeTLOGPath + "/SYNC";
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeTLOGPathKey) == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion znode path cleared (no deletion) " + zNodeTLOGPathKey);
                   return;
                }
                if (LOG.isTraceEnabled()) LOG.trace("Trafodion znode path cleared (deletion) " + zNodeTLOGPathKey);
                ZKUtil.deleteNode(zooKeeper, zNodeTLOGPathKey);
            } catch (KeeperException e) {
                throw new IOException("HBasePeerTmZK:setTLOGSyncNode: ZKW Unable to set TLOG-SYNC zNode: " + zNodeTLOGPathKey +"  , throwing IOException " + e);
            }
        }

        /**
         ** @param data
         ** #throws IOException
         **/
        public void createTLOGSyncNode(byte [] data) throws IOException {
            String zNodeTLOGPathKey = zNodeTLOGPath + "/SYNC";
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeTLOGPathKey) == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion TLOG Sync node does not exist " + zNodeTLOGPathKey);
                    ZKUtil.createWithParents(zooKeeper, zNodeTLOGPath);
                }
                ZKUtil.createAndWatch(zooKeeper, zNodeTLOGPathKey, data);
            } catch (KeeperException e) {
                throw new IOException("HBasePeerTmZK: ZKW Unable to create TLOG Sync zNode: " + zNodeTLOGPathKey + "  , throwing IOException " + e);
            }
        }

    	/**
    	 * @param znode
    	 * @return
    	 * @throws KeeperException
    	 */
    	public boolean isTLOGReady() throws IOException {
            String zNodeTLOGPathKey = zNodeTLOGPath + "/SYNC";
            try {
                if (ZKUtil.checkExists(zooKeeper, zNodeTLOGPathKey) == -1) {
                   if (LOG.isTraceEnabled()) LOG.trace("Trafodion znode path does not exist " + zNodeTLOGPathKey);
                   return false;
                }
                if (LOG.isTraceEnabled()) LOG.trace("Trafodion znode path exists " + zNodeTLOGPathKey);
            } catch (KeeperException e) {
                throw new IOException("HBasePeerTmZK:setTLOGSyncNode: ZKW Unable to set TLOG-SYNC zNode: " + zNodeTLOGPathKey +"  , throwing IOException " + e);
            }
            return true;
    	}
        
	/**
	 * @param node
	 * @param recovTable
	 * @throws IOException
	 */
        public void postAllRegionEntries(HTable recovTable) throws IOException {
           LOG.info("HBaseTmZK:postAllRegionEntries: recovTable: " + recovTable );
           NavigableMap<HRegionInfo, ServerName> regionMap = recovTable.getRegionLocations();
           Iterator<Map.Entry<HRegionInfo, ServerName>> it =  regionMap.entrySet().iterator();
           while(it.hasNext()) { // iterate entries.
              NavigableMap.Entry<HRegionInfo, ServerName> pairs = it.next();
              HRegionInfo region = pairs.getKey();
              LOG.info("postAllRegionEntries: region: " + region.getRegionNameAsString());
              ServerName serverValue = regionMap.get(region);
              String hostAndPort = new String(serverValue.getHostAndPort());
              StringTokenizer tok = new StringTokenizer(hostAndPort, ":");
              String hostName = new String(tok.nextElement().toString());
              int portNumber = Integer.parseInt(tok.nextElement().toString());
              byte [] lv_byte_region_info = region.toByteArray();
              LOG.info("Calling createRecoveryzNode for encoded region: " + region.getEncodedName());
              createRecoveryzNode(hostName, portNumber, region.getEncodedName(), lv_byte_region_info);
           }// while
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
	System.out.println("HBaseTMZK [<options>...]");
	System.out.println("<options>         : [ <peer info> | -h | -v ]");
	System.out.println("<peer info>       : -peer <id>");
	System.out.println("                  :    With this option the command is executed at the specified peer.");
	System.out.println("                  :    (Defaults to the local cluster)");
	System.out.println("-h                : Help (this output).");
	System.out.println("-v                : Verbose output. ");

    }    

    public static void main(String [] Args) throws Exception {

	boolean lv_retcode = true;

	boolean lv_verbose = false;
	boolean lv_test    = false;
	int     lv_peer_id = 0;

	String  lv_my_id  = new String();

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
	    if (lv_arg.compareTo("-t") == 0) {
		lv_test = true;
	    }
	    else if (lv_arg.compareTo("-peer") == 0) {
		lv_peer_id = Integer.parseInt(Args[lv_index]);
		if (lv_verbose) System.out.println("Talk to Peer Cluster, ID: " + lv_peer_id);
	    }
	}
	    
	STRConfig lv_STRConfig = null;
	Configuration lv_config = HBaseConfiguration.create();
	if (lv_peer_id > 0) {
	    try {
		lv_STRConfig = STRConfig.getInstance(lv_config);
		lv_config = lv_STRConfig.getPeerConfiguration(lv_peer_id, false);
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

	HBaseTmZK lv_tz = new HBaseTmZK(lv_config);
	lv_tz.printChildren(HBaseTmZK.rootNode, 0);

	System.exit(0);
    }

}
