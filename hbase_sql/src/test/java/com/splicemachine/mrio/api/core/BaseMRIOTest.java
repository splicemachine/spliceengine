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

    protected static HRegionLocation getRegionLocation(String tableNameAsString, HBaseAdmin hBaseAdmin) throws IOException, SQLException {
        TableName tableName = TableName.valueOf("splice",sqlUtil.getConglomID(tableNameAsString));
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

	protected static void move(String tableNameAsString, HBaseAdmin hBaseAdmin) throws SQLException, IOException {
        String conglomId = sqlUtil.getConglomID(tableNameAsString);
        TableName tableName1 = TableName.valueOf("splice",conglomId);
		HBaseAdmin admin = hBaseAdmin;
		try {
            if (admin == null) {
                admin = new HBaseAdmin(config);
            }
            List<HRegionInfo> tableRegions = admin.getTableRegions(tableName1);
            byte[] encodedRegionNameBytes = null;
            byte[] encodedRegionNameStr = null;
            String serverNameStr = null;
            System.out.println();
            for (HRegionInfo r : tableRegions) {
                System.out.println("=================");
                System.out.println(r.getRegionNameAsString());
                System.out.println(r.getEncodedName());
                encodedRegionNameStr = r.getRegionName();
                encodedRegionNameBytes = r.getEncodedNameAsBytes();
                System.out.println(r.getRegionId());
                System.out.println(r.getTable().getNameAsString());
                System.out.println("=================");
            }
            Collection<ServerName> rs = admin.getClusterStatus().getServers();
            for (ServerName s : rs) {
                System.out.println(s.getHostname());
                System.out.println(s.getHostAndPort());
                System.out.println(s.getServerName());
                serverNameStr = s.getServerName();
                System.out.println(s.getStartcode());
                System.out.println("+++++++++++++++++");
            }

            HRegionLocation loc =MetaTableAccessor.getRegionLocation(admin.getConnection(), encodedRegionNameStr);
            System.out.println(loc.toString());
            System.out.println(loc.getRegionInfo());
            System.out.println(loc.getRegionInfo().getRegionNameAsString());
            System.out.println(loc.getServerName().getServerName());

            System.out.println("Requesting flush...");
            admin.flush(tableName1);
            System.out.println("Flush finished...");

            System.out.println("Requesting move...");
            admin.move(encodedRegionNameBytes, Bytes.toBytes(serverNameStr));
            System.out.println("Move finished");

            tableRegions = admin.getTableRegions(tableName1);
            System.out.println();
            for (HRegionInfo r : tableRegions) {
                System.out.println("*****************");
                System.out.println(r.getRegionNameAsString());
                System.out.println(r.getEncodedName());
                System.out.println(r.getRegionId());
                System.out.println(r.getTable().getNameAsString());
                System.out.println("*****************");
            }

        } finally {
    		if (admin != null)
    			admin.close();

			}
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
