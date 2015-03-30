package com.splicemachine.hbase;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.MiniHBaseCluster;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.snapshot.SnapshotTestingUtils;

public class HBaseSupportImpl implements HBaseSupport {

	public HBaseSupportImpl(){}
	
	@Override
	public boolean tableExists(HBaseAdmin admin, byte[] tableName)
			throws IOException {
		return admin.tableExists(tableName);
	}

	@Override
	public void deleteTable(HBaseTestingUtility util, byte[] tableName)
			throws IOException 
	{
		util.deleteTable(tableName);
	}

	@Override
	public void snapshot(HBaseAdmin admin, byte[] snapshot, byte[] table)
			throws IOException 
	{
		admin.snapshot(snapshot, table);
	}

	@Override
	public List<HRegion> getRegions(MiniHBaseCluster cluster, byte[] tableName)
			throws IOException {
		return cluster.getRegions(tableName);
	}

	@Override
	public void createTable(HBaseTestingUtility util, byte[] tableName,
			byte[] family) throws IOException 
	{
		try{
			SnapshotTestingUtils.createTable(util, tableName, family);
		} catch(InterruptedException e){
			throw new IOException(e);
		}
	}

	@Override
	public HTable newTable(Configuration conf, byte[] tableName)
			throws IOException 
	{
		return new HTable(conf, tableName);
	}

}
