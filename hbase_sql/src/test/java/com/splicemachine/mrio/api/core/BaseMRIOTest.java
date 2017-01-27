/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Ignore;

import com.splicemachine.access.HBaseConfigurationSource;
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
