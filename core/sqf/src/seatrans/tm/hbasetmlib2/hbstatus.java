// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2015 Hewlett-Packard Development Company, L.P.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.dtm;

import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableExistsException;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.log4j.PropertyConfigurator;

/* 

 Checks the status of the HBase environment

 Usage: $JAVA_HOME/bin/java hbstatus [table name pattern]

 Checks the status of HMaster.
 Gets the number (and info) of the Region Servers.
 Gets the status of the tables (based on the pattern that is passed as a parameter).
 By default, it checks for the status of the Trafodion meta tables [TRAFODION._META_.*]
 
*/

public class hbstatus {

    private static final Log LOG = LogFactory.getLog(hbstatus.class);

    public static boolean CheckStatus(String pv_pattern) throws Exception {

	boolean lv_metadata_tables_fine = true;

	Configuration lv_config = HBaseConfiguration.create();
	lv_config.setInt("hbase.client.retries.number", 3);
	lv_config.set("hbase.root.logger","ERROR,console");
	System.out.println("ZooKeeper Quorum: " + lv_config.get("hbase.zookeeper.quorum"));
	System.out.println("ZooKeeper Port  : " + lv_config.get("hbase.zookeeper.property.clientPort"));

        System.out.println("Checking if HBase is available...");
        try {
            HBaseAdmin.checkHBaseAvailable(lv_config);
        }
	catch (MasterNotRunningException me) {
            System.out.println("HBase Master is not running");
	    System.out.println("HBase is not available");
	    return false;
	}
        catch (Exception e) {
            System.out.println("Caught an exception in HBaseAdmin.checkHBaseAvailable: " + e);
	    System.out.println("HBase is not available");
            return false;
        }

        System.out.println("\nHBase is available!");
	HBaseAdmin lv_admin = new HBaseAdmin(lv_config);

	ClusterStatus lv_cs = lv_admin.getClusterStatus();
	System.out.println("HMaster: " + lv_cs.getMaster());

	Collection<ServerName> lv_csn = lv_cs.getServers();
	System.out.println("\nNumber of RegionServers available:" + lv_csn.size());
	if (lv_csn.size() <=0 ) {
	    System.out.println("Not a single RegionServer is available at the moment. Exitting...");
	    return false;
	}
	System.out.println();

	int lv_rs_count=0;
	for (ServerName lv_sn : lv_csn) {
	    System.out.println("RegionServer #" + ++lv_rs_count + ": " + lv_sn);
	}

	System.out.println();
	if (pv_pattern.length() > 0) {
	    HTableDescriptor[] la_tables = lv_admin.listTables(pv_pattern);
	    System.out.println("Number of user tables matching the pattern: "
			       + pv_pattern
			       + ":" + la_tables.length);

	    for (HTableDescriptor lv_table: la_tables) {
		List<HRegionInfo> lv_lhri = lv_admin.getTableRegions(lv_table.getTableName());
		System.out.println("========================================================");
		System.out.println("Table:" 
				   + lv_table 
				   + ":#regions:" 
				   + lv_lhri.size()
				   + ":" + (lv_admin.isTableAvailable(lv_table.getTableName()) ? "Available":"Not Available")
				   + ":" + (lv_admin.isTableDisabled(lv_table.getTableName()) ? "Disabled":"Enabled")
				   );
		int lv_region_count=0;
		for (HRegionInfo lv_hri: lv_lhri) {
		    System.out.println("Region#" 
				       + ++lv_region_count 
				       + ":" + lv_hri 
				       + ":" + (lv_hri.isOffline() ? "Offline":"Online"));
		}
	    }
	}
	else {
	    System.out.println("Checking the status of Trafodion metadata tables...");
	    HTableDescriptor[] la_tables = lv_admin.listTables("TRAFODION._MD_.*");
	    System.out.println("Trafodion metadata tables: #: " + la_tables.length);

	    for (HTableDescriptor lv_table: la_tables) {
		boolean lv_table_available = lv_admin.isTableAvailable(lv_table.getTableName());
		boolean lv_table_disabled = lv_admin.isTableDisabled(lv_table.getTableName());
		if ( (!lv_table_available) || lv_table_disabled) {
		    lv_metadata_tables_fine = false;
		    System.out.println("Table :" + lv_table 
				       + ":" + (lv_table_available ? "Available":"Not Available") 
				       + ":" + (lv_table_disabled ? "Disabled":"Enabled"));
				       
		}
		List<HRegionInfo> lv_lhri = lv_admin.getTableRegions(lv_table.getTableName());
		for (HRegionInfo lv_hri: lv_lhri) {
		    if (lv_hri.isOffline()) {
			lv_metadata_tables_fine = false;
			System.out.println( "Table :" + lv_table 
					    + ": Region: " + lv_hri + " is Offline");
		    }
		}
	    }

	    if (la_tables.length > 0) {
		System.out.print("Trafodion Metadata Table status:");
		if (!lv_metadata_tables_fine) {
		    System.out.println("Not Good");
		    System.out.println("Not all the Trafodion Metadata Tables are online / available / enabled");
		}
		else {
		    System.out.println("Good");
		    System.out.println("All the Trafodion Metadata Tables are online / available / enabled");
		}
	    }
	}

	return true && lv_metadata_tables_fine;
    }

    public static void main(String[] Args) {

      String pv_pattern;
      boolean lv_retcode = true;

      if (Args.length <= 0) {
	  pv_pattern = new String("");
      }
      else {
	  pv_pattern = Args[0];
      }

      try {
	  lv_retcode = CheckStatus(pv_pattern);
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
