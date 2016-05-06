/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.trafodion.dcs.Constants;
import org.trafodion.dcs.master.listener.ListenerService;
import org.trafodion.dcs.util.DcsConfiguration;
import org.trafodion.dcs.util.Bytes;
import org.trafodion.dcs.util.DcsNetworkConfiguration;
import org.trafodion.dcs.util.InfoServer;
import org.trafodion.dcs.util.VersionInfo;
import org.trafodion.dcs.zookeeper.ZKConfig;
import org.trafodion.dcs.zookeeper.ZkClient;

public class DcsMaster implements Runnable {
    private static final Log LOG = LogFactory.getLog(DcsMaster.class);
    private Thread thrd;
    private ZkClient zkc = null;
    private Configuration conf;
    private DcsNetworkConfiguration netConf;
    private String[] args;
    private String instance = null;
    private int port;
    private int portRange;
    private InfoServer infoServer;
    private String serverName;
    private int infoPort;
    private long startTime;
    private ServerManager serverManager;
    private ListenerService ls;
    public static final String MASTER = "master";
    private Metrics metrics;
    private String parentZnode;
    private ExecutorService pool = null;
    private JVMShutdownHook jvmShutdownHook;
    private static String trafodionHome;
    private CountDownLatch isLeader = new CountDownLatch(1);

    private MasterLeaderElection mle = null;

    private class JVMShutdownHook extends Thread {
        public void run() {
            LOG.debug("JVM shutdown hook is running");
            try {
                zkc.close();
            } catch (InterruptedException ie) {
            }
            ;
        }
    }

    public DcsMaster(String[] args) {
        this.args = args;
        conf = DcsConfiguration.create();
        port = conf.getInt(Constants.DCS_MASTER_PORT,
                Constants.DEFAULT_DCS_MASTER_PORT);
        portRange = conf.getInt(Constants.DCS_MASTER_PORT_RANGE,
                Constants.DEFAULT_DCS_MASTER_PORT_RANGE);
        parentZnode = conf.get(Constants.ZOOKEEPER_ZNODE_PARENT,
                Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT);
        trafodionHome = System.getProperty(Constants.DCS_TRAFODION_HOME);
        jvmShutdownHook = new JVMShutdownHook();
        Runtime.getRuntime().addShutdownHook(jvmShutdownHook);
        thrd = new Thread(this);
        thrd.start();
    }

    public void run() {
        VersionInfo.logVersion();

        Options opt = new Options();
        CommandLine cmd;
        try {
            cmd = new GnuParser().parse(opt, args);
            String s = cmd.getArgList().get(0).toString();
            Integer i = Integer.parseInt(s);
            instance = s;
        } catch (NumberFormatException nfe) {
            instance = "1";
        } catch (NullPointerException e) {
            LOG.error("No args found: ", e);
            System.exit(1);
        } catch (ParseException e) {
            LOG.error("Could not parse: ", e);
            System.exit(1);
        }

        try {
            zkc = new ZkClient();
            zkc.connect();
            LOG.info("Connected to ZooKeeper");
        } catch (Exception e) {
            LOG.error(e);
            System.exit(1);
        }

        metrics = new Metrics();
        startTime = System.currentTimeMillis();

        try {
            // Create the persistent DCS znodes
            Stat stat = zkc.exists(parentZnode, false);
            if (stat == null) {
                zkc.create(parentZnode, new byte[0],
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT, false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_PARENT,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER, false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER, false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER_LEADER,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS, false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING, false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_RUNNING,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                    false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_SERVERS_REGISTERED,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            
            stat = zkc.exists(parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PARENT,
                    false);
            if (stat == null) {
                zkc.create(parentZnode
                        + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PARENT,
                        new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
            processSLA(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_SLAS);
            processProfile(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_PROFILES);
            processMapping(parentZnode + Constants.DEFAULT_ZOOKEEPER_ZNODE_WMS_MAPPINGS);
            
        } catch (KeeperException.NodeExistsException e) {
            // do nothing...some other server has created znodes
        } catch (Exception e) {
            LOG.error(e);
            System.exit(0);
        }

        try {
            netConf = new DcsNetworkConfiguration(conf);
            serverName = netConf.getHostName();
	    if (serverName == null) {
                LOG.error("DNS Interface [" + conf.get(Constants.DCS_DNS_INTERFACE, Constants.DEFAULT_DCS_DNS_INTERFACE)
	    			+ "] configured in dcs.site.xml is not found!");
                System.exit(1);
            }

            // Wait to become the leader of all DcsMasters
            mle = new MasterLeaderElection(this);
            isLeader.await();

            String path = parentZnode
                    + Constants.DEFAULT_ZOOKEEPER_ZNODE_MASTER + "/"
                    + netConf.getHostName() + ":" + port + ":" + portRange
                    + ":" + startTime;
            zkc.create(path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.EPHEMERAL);
            Stat stat = zkc.exists(path, false);
			if (stat == null) {
				LOG.error("Node [" + path + "] does not exist!");
				throw new Exception("Node [" + path + "] does not exist!");
			}
            long currCTime = stat.getCtime();
            
            LOG.info("Created znode [" + path + "]");

            int requestTimeout = conf.getInt(
                    Constants.DCS_MASTER_LISTENER_REQUEST_TIMEOUT,
                    Constants.DEFAULT_LISTENER_REQUEST_TIMEOUT);
            int selectorTimeout = conf.getInt(
                    Constants.DCS_MASTER_LISTENER_SELECTOR_TIMEOUT,
                    Constants.DEFAULT_LISTENER_SELECTOR_TIMEOUT);
            ls = new ListenerService(zkc, netConf, port, portRange,
                    requestTimeout, selectorTimeout, metrics, parentZnode);
            LOG.info("Listening for clients on port [" + port + "]");
            serverName = netConf.getHostName();

            // Start the info server.
            infoPort = conf.getInt(Constants.DCS_MASTER_INFO_PORT,
                    Constants.DEFAULT_DCS_MASTER_INFO_PORT);
            if (infoPort >= 0) {
                String a = conf.get(Constants.DCS_MASTER_INFO_BIND_ADDRESS,
                        Constants.DEFAULT_DCS_MASTER_INFO_BIND_ADDRESS);
                infoServer = new InfoServer(MASTER, a, infoPort, false, conf);
                infoServer.addServlet("status", "/master-status",
                        MasterStatusServlet.class);
                infoServer.setAttribute(MASTER, this);
                infoServer.start();
            }

            pool = Executors.newSingleThreadExecutor();
            serverManager = new ServerManager(this, conf, zkc, netConf,
            		currCTime, metrics);
            Future future = pool.submit(serverManager);
            future.get();// block

        } catch (Exception e) {
            LOG.error(e);
            e.printStackTrace();
            if (pool != null)
                pool.shutdown();
            System.exit(0);
        }
    }
    private void processSLA(String path) throws KeeperException, InterruptedException, Exception{
        Stat stat = null;
        List<String> children = null;
        
        stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node [" + path + "] does not exist!");
            throw new Exception("Node [" + path + "] does not exist!");
        }
        children = zkc.getChildren(path, null);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(child.equals(Constants.DEFAULT_WMS_SLA_NAME))
                    return;
            }
        } 
        createDefaultSLA(path + "/" + Constants.DEFAULT_WMS_SLA_NAME );
    }
    private void createDefaultSLA(String path) throws KeeperException, InterruptedException{
            
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%d:%s=%s:%s=%s:%s=%s:%s=%s:%s=%d",
                    Constants.IS_DEFAULT,"yes",
                    Constants.PRIORITY, 5, 
                    Constants.LIMIT, "",
                    Constants.THROUGHPUT, "",
                    Constants.ON_CONNECT_PROFILE, Constants.DEFAULT_WMS_PROFILE_NAME,
                    Constants.ON_DISCONNECT_PROFILE, Constants.DEFAULT_WMS_PROFILE_NAME,
                    Constants.LAST_UPDATE, startTime
                    ));
        
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    private void processProfile(String path) throws KeeperException, InterruptedException, Exception{
        Stat stat = null;
        List<String> children = null;
        
        stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node [" + path + "] does not exist!");
            throw new Exception("Node [" + path + "] does not exist!");
        }
        children = zkc.getChildren(path, null);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(child.equals(Constants.DEFAULT_WMS_PROFILE_NAME))
                    return;
            }
        } 
        createDefaultProfile(path + "/" + Constants.DEFAULT_WMS_PROFILE_NAME );
    }
    private void createDefaultProfile(String path) throws KeeperException, InterruptedException{
        
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%d",
                Constants.IS_DEFAULT,"yes",
                Constants.LAST_UPDATE, startTime
                ));
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }
    private void processMapping(String path) throws KeeperException, InterruptedException, Exception{
        Stat stat = null;
        List<String> children = null;
        
        stat = zkc.exists(path, false);
        if (stat == null) {
            zkc.create(path,
                    new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                    CreateMode.PERSISTENT);
            stat = zkc.exists(path, false);
        }
        if (stat == null) {
            LOG.error("Node [" + path + "] does not exist!");
            throw new Exception("Node [" + path + "] does not exist!");
        }
        children = zkc.getChildren(path, null);
        if( ! children.isEmpty()){ 
            for(String child : children) {
                if(child.equals(Constants.DEFAULT_WMS_MAPPING_NAME))
                    return;
            }
        } 
        createDefaultMapping(path + "/" + Constants.DEFAULT_WMS_MAPPING_NAME );
    }
    private void createDefaultMapping(String path) throws KeeperException, InterruptedException{
        
        byte data[] = Bytes.toBytes(String.format("%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%s:%s=%d:%s=%s",
                Constants.IS_ACTIVE,"yes",
                Constants.SLA,Constants.DEFAULT_WMS_SLA_NAME,
                Constants.USER_NAME, "",
                Constants.APPLICATION_NAME, "",
                Constants.SESSION_NAME, "",
                Constants.ROLE_NAME, "",
                Constants.CLIENT_IP_ADDRESS, "",
                Constants.CLIENT_HOST_NAME, "",
                Constants.LAST_UPDATE, startTime,
                Constants.ORDER_NUMBER, Constants.DEFAULT_ORDER_NUMBER
                ));
        zkc.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    }

    public String getServerName() {
        return serverName;
    }

    public int getInfoPort() {
        return infoPort;
    }

    public Configuration getConfiguration() {
        return conf;
    }

    public ServerManager getServerManager() {
        return serverManager;
    }

    public ListenerService getListenerService() {
        return ls;
    }

    public long getStartTime() {
        return startTime;
    }

    public int getPort() {
        return port;
    }

    public int getPortRange() {
        return portRange;
    }

    public String getZKQuorumServersString() {
        return ZKConfig.getZKQuorumServersString(conf);
    }

    public String getZKParentZnode() {
        return parentZnode;
    }

    public String getMetrics() {
        return metrics.toString();
    }

    public String getTrafodionHome() {
        return trafodionHome;
    }

    public ZkClient getZkClient() {
        return zkc;
    }

    public String getInstance() {
        return instance;
    }

    public boolean isFollower() {
        return mle.isFollower();
    }

    public void setIsLeader() {
        isLeader.countDown();
    }

    public DcsNetworkConfiguration getNetConf() {
        return netConf;
    }

    public static void main(String[] args) {
        DcsMaster server = new DcsMaster(args);
    }
}
