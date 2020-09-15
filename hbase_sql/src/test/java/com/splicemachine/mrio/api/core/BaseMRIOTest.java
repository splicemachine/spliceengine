/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.log4j.Logger;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.mrio.MRConstants;

public class BaseMRIOTest extends SpliceUnitTest{
	private static final Logger LOG = Logger.getLogger(BaseMRIOTest.class);
	protected static Configuration config;
	protected static SMSQLUtil sqlUtil;
	protected static org.apache.hadoop.hbase.client.Connection connection;

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
		try {
			connection = ConnectionFactory.createConnection(config);
		} catch (IOException e) {
			LOG.error(e.getMessage());
		}
	}

	protected static void flushTable(String tableName) throws SQLException, IOException, InterruptedException {
		TableName spliceTableName = TableName.valueOf("splice", sqlUtil.getConglomID(tableName));
		try (Admin admin = connection.getAdmin()) {
			admin.flush(spliceTableName);
		}
	}


	protected static void compactTable(String tableName) throws SQLException, IOException, InterruptedException {
		TableName conglomId = TableName.valueOf(sqlUtil.getConglomID(tableName));
		try (Admin admin = connection.getAdmin()) {
			admin.majorCompact(conglomId);
		}
	}

	protected static void splitTable(String tableName) throws SQLException, IOException, InterruptedException {
		TableName spliceTableName = TableName.valueOf("splice", sqlUtil.getConglomID(tableName));
		try (Admin admin = connection.getAdmin()) {
			admin.split(spliceTableName);
			while (admin.getRegions(spliceTableName).size() < 2) {
				Thread.sleep(1000); // wait for split to complete
				try {
					admin.split(spliceTableName); // just in case
				} catch (Exception e) {
				}
			}
		}
	}

	protected static HRegionLocation getRegionLocation(String conglomId, Admin hBaseAdmin) throws IOException, SQLException {
		TableName tableName = TableName.valueOf("splice",conglomId);
		List<HRegionInfo> tableRegions = hBaseAdmin.getTableRegions(tableName);
		if (tableRegions == null || tableRegions.isEmpty()) {
			return null;
		}
		byte[] encodedRegionNameBytes = tableRegions.get(0).getRegionName();
		return MetaTableAccessor.getRegionLocation(hBaseAdmin.getConnection(), encodedRegionNameBytes);
	}

	protected static int getRegionCount(String tableName) throws SQLException, IOException, InterruptedException {
		TableName spliceTableName = TableName.valueOf("splice", sqlUtil.getConglomID(tableName));
		try (Admin admin = connection.getAdmin()) {
			return admin.getRegions(spliceTableName).size();
		}
	}

	protected static Collection<ServerName> getAllServers(Admin hBaseAdmin) throws IOException, SQLException {
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
