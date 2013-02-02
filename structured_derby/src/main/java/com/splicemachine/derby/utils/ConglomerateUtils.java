package com.splicemachine.derby.utils;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SchemaConstants;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

/**
 * Utilities related to managing DerbyConglomerates
 *
 * @author Scott Fines
 * Created: 2/2/13 10:11 AM
 */
public class ConglomerateUtils {
	public static final String CONGLOMERATE_ATTRIBUTE = "DERBY_CONGLOMERATE";
	public static final String SPLIT_WAIT_INTERVAL = "splice.splitWaitInterval";
	public static final long DEFAULT_SPLIT_WAIT_INTERVAL = 500l;
	private static final Logger LOG = LoggerFactory.getLogger(ConglomerateUtils.class);

	private static final String conglomeratePath;
	private static final String conglomSequencePath;

	private static final long SLEEP_SPLIT_INTERVAL;

	static{
		conglomeratePath = SpliceUtils.config.get(SchemaConstants.CONGLOMERATE_PATH_NAME,
																							SchemaConstants.DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		conglomSequencePath = conglomeratePath+"/__CONGLOM_SEQUENCE";
		try {
			ZkUtils.addIfAbsent(conglomeratePath,new byte[0],ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
			ZkUtils.addIfAbsent(conglomSequencePath,Bytes.toBytes(0l),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		} catch (IOException e) {
			LOG.error("Unable to ensure Conglomerate base path exists, aborting",e);
			/*
			 * We can't start up without a conglomerate path, so abort the path
			 */
			throw new RuntimeException(e);
		}

		SLEEP_SPLIT_INTERVAL = SpliceUtils.config.getLong(SPLIT_WAIT_INTERVAL,DEFAULT_SPLIT_WAIT_INTERVAL);
	}



	public static <T> T readConglomerate(long conglomId, Class<T> instanceClass){
		LOG.trace("readConglomerate {}, for instanceClass {}",conglomId,instanceClass);
		String table = Long.toString(conglomId);
		byte[] data = null;
		try{
			data = ZkUtils.getData(conglomeratePath+"/"+table);
		}catch(IOException e){
			LOG.error("Unable to get Conglomerate information from ZooKeeper, attempting HBase",e);
			data = null;
		}
		if(data!=null){
			return SpliceUtils.fromJSON(Bytes.toString(data), instanceClass);
		}
			/*
			 * The data isn't in ZooKeeper, so we'll need to
			 * look it up from an HBase table
			 */
		LOG.warn("Missing conglomerate information, make sure that zookeeper is not transient");
		HBaseAdmin admin = SpliceUtils.getAdmin();
		try {
			if(admin.tableExists(table)) {
				HTableDescriptor td = admin.getTableDescriptor(Bytes.toBytes(table));
				String json = td.getValue(CONGLOMERATE_ATTRIBUTE);
				ZkUtils.setData(conglomeratePath+"/"+table,Bytes.toBytes(json),-1);
				if(LOG.isTraceEnabled())
					LOG.trace("readConglomerate {}, json {}",table,json);
				return SpliceUtils.fromJSON(json, instanceClass);
			}else{
				LOG.error("Table {} does not exist in hbase",table);
			}
		} catch (IOException e) {
			LOG.error("Unable to get Conglomerate information from HBase", e);
			throw new RuntimeException(e);
		}finally{
			Closeables.closeQuietly(admin);
		}
		return null;
	}

	public static void createConglomerate(long conglomId,
																				Conglomerate conglomerate) throws IOException {
		createConglomerate(Long.toString(conglomId),SpliceUtils.toJSON(conglomerate));
	}

	public static void createConglomerate(String tableName,
																				String conglomData) throws IOException {
		LOG.debug("creating Hbase table for conglom {} with data {}",tableName,conglomData);
		HBaseAdmin admin = SpliceUtils.getAdmin();
		try{
			HTableDescriptor td = SpliceUtils.generateDefaultDescriptor(tableName);
			admin.createTable(td);
			ZkUtils.safeCreate(conglomeratePath+"/"+tableName,Bytes.toBytes(conglomData),ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
		} catch (InterruptedException e) {
			throw new IOException(e);
		} catch (KeeperException e) {
			throw new IOException(e);
		} catch (IOException e) {
			throw new IOException(e);
		}finally{
			Closeables.closeQuietly(admin);
		}
	}

	public static void updateConglomerate(Conglomerate conglomerate) throws IOException {
		String conglomData = SpliceUtils.toJSON(conglomerate);
		String table = Long.toString(conglomerate.getContainerid());
		LOG.debug("updating table {} in hbase with serialized data {}",table,conglomData);
		HBaseAdmin admin = SpliceUtils.getAdmin();
		try{
			if(!admin.tableExists(Bytes.toBytes(table))){
				LOG.error("Unable to update table {}, it does not exist in hbase",conglomerate.getContainerid());
			}else{
				ZkUtils.setData(conglomeratePath+"/"+table,Bytes.toBytes(conglomData),-1);
			}
		}finally{
			Closeables.closeQuietly(admin);
		}
	}

	public static long getNextConglomerateId() throws IOException{
		LOG.trace("getting next conglomerate id");
		return ZkUtils.nextSequenceId(conglomSequencePath);
	}

	public static void splitConglomerate(long conglomId) throws IOException, InterruptedException {
		HBaseAdmin admin = SpliceUtils.getAdmin();
		admin.split(Bytes.toBytes(Long.toString(conglomId)));
	}

	public static void splitConglomerate(long conglomId,
																			 byte[] position)
											throws IOException, InterruptedException {
		HBaseAdmin admin = SpliceUtils.getAdmin();
		byte[] name = Bytes.toBytes(Long.toString(conglomId));
		admin.split(name,position);

		boolean isSplitting=true;
		while(isSplitting){
			isSplitting=false;
			List<HRegionInfo> regions = admin.getTableRegions(name);
			for(HRegionInfo region:regions){
				if(region.isSplit()){
					isSplitting=true;
					break;
				}
			}
			Thread.sleep(SLEEP_SPLIT_INTERVAL);
		}
	}
}
