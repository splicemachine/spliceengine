package com.splicemachine.derby.utils;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SchemaConstants;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
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
			ZkUtils.safeCreate(conglomeratePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			ZkUtils.safeCreate(conglomSequencePath, Bytes.toBytes(0l), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			LOG.error("Unable to ensure Conglomerate base path exists, aborting",e);
			/*
			 * We can't start up without a conglomerate path, so abort the path
			 */
			throw new RuntimeException(e);
		} catch (InterruptedException e) {
			LOG.error("Interrupted while creating conglomerate paths,aborting", e);
			/*
			 * We can't start up without a conglomerate path, so abort the path
			 */
			throw new RuntimeException(e);
		}

		SLEEP_SPLIT_INTERVAL = SpliceUtils.config.getLong(SPLIT_WAIT_INTERVAL, DEFAULT_SPLIT_WAIT_INTERVAL);
	}

	/**
	 * Reads stored Conglomerate information and returns it as an instance of {@code instanceClass}.
	 *
	 * @param conglomId the id of the conglomerate
	 * @param instanceClass the type to return
	 * @param <T> the type to return
	 * @return an instance of {@code T} which contains the conglomerate information.
	 */
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

	/**
	 * Stores information about a new conglomerate, specified by {@code conglomId}.
	 *
	 * @param conglomId the conglom id to store information under
	 * @param conglomerate the conglomerate to store
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
	public static void createConglomerate(long conglomId,
																				Conglomerate conglomerate) throws IOException {
		createConglomerate(Long.toString(conglomId),SpliceUtils.toJSON(conglomerate));
	}

	/**
	 * Stores information about a new conglomerate, specified by {@code tableName}.
	 *
	 * @param tableName the name of the table
	 * @param conglomerate the conglomerate to store
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
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

	/**
	 * Update a conglomerate.
	 *
	 * @param conglomerate the new conglomerate information to update
	 * @throws IOException if something goes wrong and the data can't be stored.
	 */
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

	/**
	 * Gets the next conglomerate id for the system.
	 *
	 * @return the next conglomerate id for the system.
	 * @throws IOException if something goes wrong and the next id can't be fetched.
	 */
	public static long getNextConglomerateId() throws IOException{
		LOG.trace("getting next conglomerate id");
		return ZkUtils.nextSequenceId(conglomSequencePath);
	}

	/**
	 * Split a conglomerate. This is an asynchronous operation.
	 *
	 * @param conglomId the conglomerate to split.
	 * @throws IOException if something goes wrong and the split fails
	 * @throws InterruptedException if the split is interrupted.
	 */
	public static void splitConglomerate(long conglomId) throws IOException, InterruptedException {
		HBaseAdmin admin = SpliceUtils.getAdmin();
		admin.split(Bytes.toBytes(Long.toString(conglomId)));
	}

	/**
	 * Synchronously split a conglomerate around a specific row position.
	 *
	 * This method will block until it detects that the split is completed. Unfortunately,
	 * it must block via polling. For a more responsive version, change the setting
	 * "splice.splitWaitInterval" in splice-site.xml.
	 *
	 * @param conglomId the id of the conglomerate to split
	 * @param position the row to split around
	 * @throws IOException if something goes wrong and the split fails
	 * @throws InterruptedException if the split operation is interrupted.
	 */
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
