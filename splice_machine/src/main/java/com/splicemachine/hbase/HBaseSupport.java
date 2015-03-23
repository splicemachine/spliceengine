package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * This interface to support 
 * different API versions of HBase : 0.94 and 0.98
 * @author vrodionov
 *
 */
public interface HBaseSupport {

	public boolean tableExists(HBaseAdmin admin, byte[] tableName) throws IOException;
	public void deleteTable(HBaseTestingUtility util, byte[] tableName) throws IOException;
	public void snapshot (HBaseAdmin admin, byte[] snapshot, byte[] table) throws IOException;
	public List<HRegion> getRegions(MiniHBaseCluster cluster, byte[] tableName) throws IOException;
	public void createTable(HBaseTestingUtility util, byte[] tabkeName, byte[] family) throws IOException;
	public HTable newTable(Configuration conf, byte[] tableName) throws IOException;
}
