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

package org.trafodion.sql;

import java.io.IOException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.lang.reflect.Field;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
// These are needed for the DDL_TIME constant. This class is different in Hive 0.10.
// We use Java reflection instead of importing the class statically. 
// For Hive 0.9 or lower
// import org.apache.hadoop.hive.metastore.api.Constants;
// For Hive 0.10 or higher
// import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataOutputStream;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;


public class HiveClient {
    static Logger logger = Logger.getLogger(HiveClient.class.getName());
    static String ddlTimeConst = null;
    String lastError;
    HiveConf hiveConf = null;
    HiveMetaStoreClient hmsClient  ;
    FSDataOutputStream fsOut = null;

    public HiveClient() {
   
    }

    public String getLastError() {
        return lastError;
    }

    void setLastError(String err) {
        lastError = err;
    }

    void setupLog4j() {
        String confFile = System.getenv("MY_SQROOT")
            + "/conf/log4j.hdfs.config";
        PropertyConfigurator.configure(confFile);
    }

    public boolean init(String metastoreURI) 
              throws MetaException {
         setupLog4j();
         if (logger.isDebugEnabled()) logger.debug("HiveClient.init(" + metastoreURI + " " + ") called.");
         ddlTimeConst = getDDLTimeConstant();
         hiveConf = new HiveConf();
	 if (metastoreURI.length() > 0) {
             hiveConf.set("hive.metastore.local", "false");
             hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, metastoreURI);
         }
         hmsClient = new HiveMetaStoreClient(hiveConf, null);
         return true;
    }

    public boolean close() {
	if (logger.isDebugEnabled()) logger.debug("HiveClient.close called.");
        hmsClient.close();
        return true;
    }

    public boolean exists(String schName, String tblName)  
        throws MetaException, TException, UnknownDBException {
            if (logger.isDebugEnabled()) logger.debug("HiveClient.exists(" + schName + " , " + tblName + ") called.");
            boolean result = hmsClient.tableExists(schName, tblName);
            return result;
    }

    public String getHiveTableString(String schName, String tblName)
        throws MetaException, TException {
        Table table;
        String result;

        if (logger.isDebugEnabled()) logger.debug("HiveClient.getHiveTableString(" + schName + " , " + 
                     tblName + ") called.");
        try {
            table = hmsClient.getTable(schName, tblName);
        }
        catch (NoSuchObjectException x) {
            if (logger.isDebugEnabled()) logger.debug("HiveTable not found");
            return new String("");
        }
        if (logger.isDebugEnabled()) logger.debug("HiveTable is " + table.toString());
        result = table.toString();
        if (table.isSetPartitionKeys()) {
            // append the string representation of each partition
            // to the table string
            List<Partition> partns = hmsClient.listPartitions(schName, tblName, (short) 9999);
            for (int i=0; i < partns.size(); i++)
                result = result + ", " + partns.get(i).toString();
        }
        return result;
    }

    public long getRedefTime(String schName, String tblName)
        throws MetaException, TException, ClassCastException, NullPointerException, NumberFormatException {
        Table table;
        if (logger.isDebugEnabled()) logger.debug("HiveClient.getRedefTime(" + schName + " , " + 
                     tblName + ") called.");
        try {
            table = hmsClient.getTable(schName, tblName);
            if (table == null) {
		if (logger.isDebugEnabled()) logger.debug("getTable returns null for " + schName + "." + tblName + ".");
                return 0;
	    }
        }
        catch (NoSuchObjectException x) {
            if (logger.isDebugEnabled()) logger.debug("Hive table no longer exists.");
            return 0;
        }

        long redefTime = table.getCreateTime();
        if (table.getParameters() != null){
            // those would be used without reflection
            //String rfTime = table.getParameters().get(Constants.DDL_TIME);
            //String rfTime = table.getParameters().get(hive_metastoreConstants.DDL_TIME);
            // determing the constant using reflection instead
            String rfTime = table.getParameters().get(ddlTimeConst);
            if (rfTime != null)
                redefTime = Long.parseLong(rfTime);
        }
        
        if (!table.getPartitionKeys().isEmpty()) {
        	// for a partitioned table, loop through the partitions and find
        	// the one that was created last
        	List<String> partNames = hmsClient.listPartitionNames(schName,tblName,(short) 1000);
        	
        	for (String partName : partNames) {
        		int partCreateTime =
        				hmsClient.getPartition(schName, tblName, partName).getCreateTime();
        		
        		if (partCreateTime > redefTime)
        			redefTime = partCreateTime;
        	}
        	
        }
        if (logger.isDebugEnabled()) logger.debug("RedefTime is " + redefTime);
        return redefTime ;
    }

    public Object[] getAllSchemas() throws MetaException {
        List<String> schemaList = (hmsClient.getAllDatabases());
        if (schemaList != null)
           return schemaList.toArray();
        else
           return null; 
    }

    public Object[] getAllTables(String schName) 
        throws MetaException {
        List<String> tableList = hmsClient.getAllTables(schName);
        if (tableList != null)
           return tableList.toArray();
        else
           return null;
    }

    // Because Hive changed the name of the class containing internal constants changed
    // in Hive 0.10, we are using Java Reflection to get the value of the DDL_TIME constant.
    public static String getDDLTimeConstant()
        throws MetaException {

        Class constsClass = null;
        Object constsFromReflection = null; 
        Field ddlTimeField = null;
        Object fieldVal = null;

        // Using the class loader, try to load either class by name.
        // Note that both classes have a default constructor and both have a static
        // String field DDL_TIME, so the rest of the code is the same for both.
        try { 
            try {
                constsClass = Class.forName(
                   // Name in Hive 0.10 and higher
                   "org.apache.hadoop.hive.metastore.api.hive_metastoreConstants");
            } catch (ClassNotFoundException e) { 
                // probably not found because we are using Hive 0.10 or later
                constsClass = null;
            } 
            if (constsClass == null) {
                constsClass = Class.forName(
                    // Name in Hive 0.9 and lower
                    "org.apache.hadoop.hive.metastore.api.Constants");
            }

            // Make a new object for this class, using the default constructor
            constsFromReflection = constsClass.newInstance(); 
        } catch (InstantiationException e) { 
            throw new MetaException("Instantiation error for metastore constants class");
        } catch (IllegalAccessException e) { 
            throw new MetaException("Illegal access exception");
        } catch (ClassNotFoundException e) { 
            throw new MetaException("Could not find Hive Metastore constants class");
        } 

        // Using Java reflection, get a reference to the DDL_TIME field
        try {
            ddlTimeField = constsClass.getField("DDL_TIME");
        } catch (NoSuchFieldException e) {
            throw new MetaException("Could not find DDL_TIME constant field");
        }

        // get the String object that represents the value of this field
        try {
            fieldVal = ddlTimeField.get(constsFromReflection);
        } catch (IllegalAccessException e) {
            throw new MetaException("Could not get value for DDL_TIME constant field");
        }

        return fieldVal.toString();
    }

  ///////////////////   
  boolean hdfsCreateFile(String fname) throws IOException
  {
    HiveConf  config = new HiveConf();
    if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsCreateFile() - started" );
    Path filePath = new Path(fname);
    FileSystem fs = FileSystem.get(filePath.toUri(),config);
    fsOut = fs.create(filePath, true);
    
    if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsCreateFile() - file created" );

    return true;
  }
  
  boolean hdfsWrite(byte[] buff, long len) throws Exception
  {

    if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsWrite() - started" );
    try
    {
      fsOut.write(buff);
      fsOut.flush();
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsWrite() -- exception: " + e);
      throw e;
    }
    if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsWrite() - bytes written and flushed:" + len  );
    
    return true;
  }
  
  boolean hdfsClose() throws IOException
  {
    if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsClose() - started" );
    try
    {
      fsOut.close();
    }
    catch (IOException e)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.hdfsClose() - exception:" + e);
      throw e;
    }
    return true;
  }
  
  public void executeHiveSQL(String ddl) throws ClassNotFoundException, SQLException
  {
      try
      {
          Class.forName("org.apache.hive.jdbc.HiveDriver");
      }
 
      catch(ClassNotFoundException e) 
      {
          throw e;
      }

      try 
      {
          Connection con = DriverManager.getConnection("jdbc:hive2://", "hive", "");
          Statement stmt = con.createStatement();
          stmt.execute(ddl);
      }
 
      catch(SQLException e)
      {
	  throw e;
      }
  }

  // Create a partition for an existing Hive table
  //  schemaName:      Name of Hive database or default schema "default"
  //  tableName:       Name of Hive table
  //  partitionString: Partition column values as in the directory name,
  //                   e.g. "p1=1,p2=two"
  public void createHiveTablePartition(String schemaName,
                                       String tableName,
                                       String partitionString) throws Exception
  {
    if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition(" + schemaName
                                              + ", " + tableName
                                              + ", " + partitionString + ") - started" );
    try
    {
      hmsClient.appendPartition(schemaName, tableName, partitionString);
    }
    catch (AlreadyExistsException ae)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() -- already exists");
      // do nothing, partition is already created
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() -- exception: " + e);
      throw e;
    }
    if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() - success");
    
  }
                                            
}
