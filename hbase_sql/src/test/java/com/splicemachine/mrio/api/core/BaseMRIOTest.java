/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.mrio.api.core;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.MetaTableAccessor;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.Ignore;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.mrio.MRConstants;

@Ignore("Breaks stuff")
public class BaseMRIOTest extends SpliceUnitTest{
	protected static Configuration config;
	protected static SMSQLUtil sqlUtil;
	
	static {
		config = HConfiguration.unwrapDelegate();
		config.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
		config.set(HConstants.HBASE_DIR,getHbaseRootDirectory());
        config.set("fs.default.name", "file:///"); // MapR Hack, tells it local filesystem
    	config.set(MRConstants.SPLICE_JDBC_STR, SpliceNetConnection.getDefaultLocalURL());
        System.setProperty(HConstants.HBASE_DIR, getHbaseRootDirectory());
    	System.setProperty("hive.metastore.warehouse.dir", getHiveWarehouseDirectory());
    	System.setProperty("mapred.job.tracker", "local");
    	System.setProperty("mapreduce.framework.name", "local-chicken");
    	System.setProperty("hive.exec.mode.local.auto","true");
    	System.setProperty("javax.jdo.option.ConnectionURL", "jdbc:derby:;databaseName=target/metastore_db;create=true");
		sqlUtil = SMSQLUtil.getInstance(SpliceNetConnection.getDefaultLocalURL());
	}

    protected static HBaseAdmin getHBaseAdmin() throws IOException {
        return new HBaseAdmin(config);
    }
	
	protected static void flushTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.flush(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();
    		
			}
	}

	
	protected static void compactTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.majorCompact(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();    		
			}
	}

	protected static void splitTable(String tableName) throws SQLException, IOException, InterruptedException {
		String conglomId = sqlUtil.getConglomID(tableName);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);
			admin.split(conglomId);
		} finally { 
    		if (admin != null)
    			admin.close();
 
			}
	}

    protected static HRegionLocation getRegionLocation(String conglomId, HBaseAdmin hBaseAdmin) throws IOException, SQLException {
        TableName tableName = TableName.valueOf("splice",conglomId);
        List<HRegionInfo> tableRegions = hBaseAdmin.getTableRegions(tableName);
        if (tableRegions == null || tableRegions.isEmpty()) {
            return null;
        }
        byte[] encodedRegionNameBytes = tableRegions.get(0).getRegionName();
        return MetaTableAccessor.getRegionLocation(hBaseAdmin.getConnection(), encodedRegionNameBytes);
    }

    protected static Collection<ServerName> getAllServers(HBaseAdmin hBaseAdmin) throws IOException, SQLException {
        return hBaseAdmin.getClusterStatus().getServers();
    }

    protected static ServerName getNotIn(Collection<ServerName> allServers, ServerName regionServer) {
        ServerName firstNonMatch = null;
        for (ServerName sn : allServers) {
            if (! sn.equals(regionServer)) {
                firstNonMatch = sn;
                break;
            }
        }
        return firstNonMatch;
    }

}
