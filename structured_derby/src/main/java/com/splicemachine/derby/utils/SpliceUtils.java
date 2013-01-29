package com.splicemachine.derby.utils;

import java.io.*;
import java.util.*;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.ExecIndexRow;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.store.raw.ContainerKey;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.sql.GenericStorablePreparedStatement;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.datanucleus.store.valuegenerator.UUIDHexGenerator;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.SchemaConstants;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.hbase.SpliceEngine;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationTree.OperationTreeStatus;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.store.access.ZookeeperTransaction;
import com.splicemachine.utils.SpliceLogUtils;

/**
 * Utility methods
 * @author jessiezhang
 * @author johnleach
 * @author scottfines
 */

@SuppressWarnings(value = "deprecation")
public class SpliceUtils {
	private static Logger LOG = Logger.getLogger(SpliceUtils.class);
	public static final String CONGLOMERATE_ATTRIBUTE = "DERBY_CONGLOMERATE";



	public enum SpliceConglomerate {HEAP,BTREE}
	public static Configuration config = HBaseConfiguration.create();
	protected static Gson gson = new Gson();
	protected static UUIDHexGenerator gen = new UUIDHexGenerator("Splice", null);
	protected static String quorum;
	protected static String conglomeratePath;
	protected static String transPath;
	protected static String derbyPropertyPath = "/derbyPropertyPath";
	protected static String queryNodePath = "/queryNodePath";	
	protected static RecoverableZooKeeper rzk = null;
	protected static ZooKeeperWatcher zkw = null;

	public static String getTableNameString(ContainerKey kye) {
		return "";
	}

	
	static {
		quorum = generateQuorum();
		conglomeratePath = config.get(SchemaConstants.CONGLOMERATE_PATH_NAME,SchemaConstants.DEFAULT_CONGLOMERATE_SCHEMA_PATH);	
		transPath = config.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH);
		try {
			
			zkw = (new HBaseAdmin(config)).getConnection().getZooKeeperWatcher();
			rzk = zkw.getRecoverableZooKeeper();
//			rzk = ZKUtil.connect(config, new Watcher() {			
//				@Override
//				public void process(WatchedEvent event) {					
//				}
//			});
			
			if (rzk.exists(conglomeratePath, false) == null)
				rzk.create(conglomeratePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (rzk.exists(queryNodePath, false) == null)
				rzk.create(queryNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (rzk.exists(derbyPropertyPath, false) == null)
				rzk.create(derbyPropertyPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (rzk.exists(transPath, false) == null)
				rzk.create(transPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {

		}
	}		

	public static RecoverableZooKeeper getRecoverableZooKeeper() {
		return rzk;
	}
	
	public static ZooKeeperWatcher getZooKeeperWatcher() {
		return zkw;
	}
	
	public static String getTransactionPath() {
		return transPath;
	}
	
	public static <T> T readConglomerate(long tableName, Class<T> instanceClass) {	
		if (LOG.isTraceEnabled())
			LOG.trace("readConglomerate " + tableName + ", for instanceClass " + instanceClass);
		HBaseAdmin admin = null;
		String table = Long.toString(tableName);
		try {
			byte[] data = rzk.getData(conglomeratePath +"/"+ table, false, null);			
			if (data == null) { // Lookup from HBase
				LOG.error("Missing conglomerate information, make sure you do not have a transient zookeeper");
				admin = new HBaseAdmin(config);  
				if (admin.tableExists(table)) {
					HTableDescriptor td = admin.getTableDescriptor(table.getBytes());
					String json = td.getValue(CONGLOMERATE_ATTRIBUTE);
					rzk.setData(conglomeratePath + "/" + table, Bytes.toBytes(json), -1);
					if (LOG.isTraceEnabled())
						LOG.trace("readConglomerate " + tableName + ", json " + json);
					return fromJSON(json,instanceClass);
				} else {
					LOG.error("table does not exist in hbase - "+tableName);
				}
			} else {
				return fromJSON(Bytes.toString(data),instanceClass);
			}

		} catch (MasterNotRunningException a) {
			LOG.error(a.getMessage(), a);
		} catch (IOException ie) {
			LOG.error(ie.getMessage(), ie);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (admin != null)
					admin.close();		
			} catch (Exception ie) {
				LOG.error(ie.getMessage(), ie);
			}
		}
		return null;
	}


	public static void createHTable(long tableName, Conglomerate conglomerate) {	
		createHTable(Long.toString(tableName), SpliceUtils.toJSON(conglomerate));
	}

	public static void createHTable(String tableName, String conglomerateJSON) {	
		LOG.debug("creating table in hbase for " + tableName + ", with serialized conglomerate " + conglomerateJSON);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);  
				HTableDescriptor td = generateDefaultDescriptor(tableName);
				admin.createTable(td);
				rzk.create(conglomeratePath + "/" + tableName, Bytes.toBytes(conglomerateJSON), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (MasterNotRunningException a) {
			LOG.error(a.getMessage(), a);
		} catch (IOException ie) {
			LOG.error(ie.getMessage(), ie);
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			try {
				if (admin != null)
					admin.close();	
			} catch (Exception ie) {
				LOG.error(ie.getMessage(), ie);
			}
		}
	}

	public static void updateHTable(HTableInterface htable, Conglomerate conglomerate) {
		if (htable == null)
			return;
		
		String conglomerateJSON = SpliceUtils.toJSON(conglomerate);
		LOG.debug("updating table in hbase for " + htable.getTableName() + ", with serialized conglomerate " + conglomerateJSON);
		HBaseAdmin admin = null;
		try {
			admin = new HBaseAdmin(config);  
			if (!admin.tableExists(htable.getTableName())) {
				LOG.error("table does not exist in hbase - "+Bytes.toString(htable.getTableName()));
			} else{
				rzk.setData(conglomeratePath + "/" + Bytes.toString(htable.getTableName()), Bytes.toBytes(conglomerateJSON), -1);
			} 
		} catch (MasterNotRunningException a) {
			LOG.error(a.getMessage(), a);
		} catch (IOException ie) {
			LOG.error(ie.getMessage(), ie);
		} catch (KeeperException e) {
			LOG.error(e.getMessage(), e);
			e.printStackTrace();
		} catch (InterruptedException e) {
			LOG.error(e.getMessage(), e);
			e.printStackTrace();
		}
		finally {
			try {
				if (admin != null)
					admin.close();			
			} catch (Exception ie) {
				LOG.error(ie.getMessage(), ie);
			}
		}
	}

	public static String toJSON(Object object) {
		return gson.toJson(object);
	}

	public static <T> T fromJSON(String json,Class<T> instanceClass) {
		return gson.fromJson(json, instanceClass);
	}

	public static Get createGet(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, byte[] transID) throws StandardException {
		SpliceLogUtils.trace(LOG,"createGet %s",loc.getBytes());
		try {
			Get get = new Get(loc.getBytes());
			if(validColumns!=null){
				for(int i= validColumns.anySetBit();i!=-1;i = validColumns.anySetBit(i)){
					get.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(i).getBytes());
				}
			}else{
				for(int i=0;i<destRow.length;i++){
					get.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(i).getBytes());
				}
			}

			//FIXME: need to get the isolation level
			if (transID != null) {
				get.setAttribute(TxnConstants.TRANSACTION_ID, transID);
				get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
													Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			}
			return get;
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
			return null;
		}
	}

	public static Delete cleanupNullsDelete(RowLocation loc,DataValueDescriptor[] destRow, FormatableBitSet validColumns, byte[] transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("cleanupNullsDelete row ");
		try {
			Delete delete = new Delete(loc.getBytes());
			if (transID != null)
				delete.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			int numrows = (validColumns != null ? validColumns.getLength() : destRow.length);  // bug 118
			for (int i = 0; i < numrows; i++) {
				if (validColumns.isSet(i) && destRow[i] != null && destRow[i].isNull())
					delete.deleteColumn(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes()); 
			}
			return delete;
		} catch (Exception e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static void populate(Result currentResult, DataValueDescriptor[] destRow) throws StandardException {
		SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",currentResult.raw().length,destRow.length);
		try{
			for(int i=0;i<destRow.length;i++){
				byte[] value = currentResult.getValue(HBaseConstants.DEFAULT_FAMILY_BYTES, (new Integer(i)).toString().getBytes());
				fill(value,destRow[i]);
			}
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
		}
	}
	
	public static void populate(Result currentResult, FormatableBitSet scanColumnList, DataValueDescriptor[] destRow) throws StandardException {
		SpliceLogUtils.trace(LOG,"populate current Result %s using scanColumnList %s and destRow with size %d",currentResult,scanColumnList,destRow.length);
		try {
			if(scanColumnList == null) populate(currentResult,destRow);
			else{
				for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
					byte[] value = currentResult.getValue(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(i).getBytes());
					fill(value,destRow[i]);
				}
			}
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error occurred during populate", e);
		}
	}

	public static void populate(Result currentResult, DataValueDescriptor[] destRow,
															FormatableBitSet scanList,int[] bitSetToDestRowMap) throws StandardException{
		if(scanList==null||scanList.getNumBitsSet()<=0) populate(currentResult,destRow);
		else{
			try{
				for(int i=scanList.anySetBit();i!=-1;i=scanList.anySetBit(i)){
					byte[] value = currentResult.getValue(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(i).getBytes());
					SpliceLogUtils.trace(LOG,"Attempting to place column[%d] into destRow %s",i,destRow[bitSetToDestRowMap[i]]);
					fill(value, destRow[bitSetToDestRowMap[i]]);
				}
			}catch(IOException e){
				SpliceLogUtils.logAndThrowRuntime(LOG,"Error occurred during populate",e);
			}
		}
	}

	private static void fill(byte[] value, DataValueDescriptor dvd) throws StandardException, IOException {
		if(value!=null&&dvd!=null){
			DerbyBytesUtil.fromBytes(value,dvd);
		}else if(dvd!=null){
			dvd.setToNull();
		}
	}

//	public static Put insert(DataValueDescriptor[] row, byte[] transID) throws StandardException {
//		return insert(row,gen.next().toString().getBytes(), transID);
//	}

	/**
	 * 
	 * This method adds additional fields to the initial row.  This is used for temp table writing where we want to assign a left and right.
	 * 
	 * @param row
	 * @param key
	 * @param transID
	 * @param additionalInserts
	 * @return
	 * @throws StandardException
	 */
//	public static Put insert(DataValueDescriptor[] row, byte[] key, byte[] transID, DataValueDescriptor[] additionalInserts) throws StandardException {
//		Put put = insert(row,key,transID);
//		put = insertAdditional(additionalInserts,put);
//		return put;
//	}
	
//	public static Put insertAdditional(DataValueDescriptor[] row, Put put) throws StandardException {
////		DerbyLogUtils.traceDescriptors(LOG, "insert row with key: "+ key +" and row", row);
//		try {
//			for (int i = 0; i < row.length; i++) {
//				if (row[i] != null && !row[i].isNull()) {
////					SpliceLogUtils.trace(LOG, "insert row type %s, value %s, qualifier %d",row[i].getTypeName(),row[i].getTraceString(),i);
//					put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(-(i+1))).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
//				} else {
//					SpliceLogUtils.trace(LOG,
//							"skipping row since it was null "+ (row[i]==null? "NULL":row[i].getTypeName()) +
//							", value "+ (row[i]==null? "NULL" : row[i].getTraceString()));
//				}
//			}
//			return put;
//		}
//		catch (IOException e) {
//			throw new RuntimeException(e.getMessage(), e);
//		}
//	}
	
//	public static Put insert(DataValueDescriptor[] row, byte[] key, byte[] transID) throws StandardException {
////		DerbyLogUtils.traceDescriptors(LOG, "insert row with key: "+ key +" and row", row);
//		try {
//			Put put = new Put(key);
//			for (int i = 0; i < row.length; i++) {
//				if (row[i] != null && !row[i].isNull()) {
////					SpliceLogUtils.trace(LOG, "insert row type %s, value %s, qualifier %d",row[i].getTypeName(),row[i].getTraceString(),i);
//					put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
//				} else {
//					SpliceLogUtils.trace(LOG,
//							"skipping row since it was null "+ (row[i]==null? "NULL":row[i].getTypeName()) +
//							", value "+ (row[i]==null? "NULL" : row[i].getTraceString()));
//				}
//
//			}
//			if (transID != null)
//				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
//			return put;
//		}
//		catch (IOException e) {
//			throw new RuntimeException(e.getMessage(), e);
//		}
//	}
	

//	public static Put insert(DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] transID) throws StandardException {
//		return insert(row,validColumns,gen.next().toString().getBytes(), transID);
//	}

//	public static Put insert(DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] key, byte[] transID) throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("insert row " + row + " with key " + key+" with validColumns "+validColumns);
//		try {
//			Put put = new Put(key);
//			int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
//			for (int i = 0; i < numrows; i++) {
//				if ( (validColumns != null && !validColumns.isSet(i)) || row[i] == null || row[i].isNull())
//					continue;
//
//				if (LOG.isTraceEnabled())
//					LOG.trace("insert row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
//				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
//			}
//			if (transID != null)
//				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
//			return put;
//		}
//		catch (IOException e) {
//			throw new RuntimeException(e.getMessage(), e);
//		}
//	}

//	public static Put update(RowLocation rowLocation, DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] transID) throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("insert row " + rowLocation);
//
//		try {
//			Put put = new Put(rowLocation.getBytes());
//			int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
//			for (int i = 0; i < numrows; i++) {
//				if ((validColumns != null && !validColumns.isSet(i)) || row[i] == null || row[i].isNull())
//					continue;
//
//				if (LOG.isTraceEnabled())
//					LOG.trace("update row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
//				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
//			}
//			if (transID != null)
//				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
//			return put;
//		}
//		catch (IOException e) {
//			throw new RuntimeException(e.getMessage(), e);
//		}
//	}

	public static boolean update(RowLocation loc, DataValueDescriptor[] row,
			FormatableBitSet validColumns, HTableInterface htable, byte[] transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("update row " + row);

		try {
			//FIXME: Check if the record exists. Not using htable.checkAndPut because it's one column at a time
			//May need to read more HTableInteface's checkAndPut
			Get get = new Get(loc.getBytes());
			
			//FIXME: need to get the isolation level
			if (transID != null) {
				get.setAttribute(TxnConstants.TRANSACTION_ID, transID);
				get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL, 
		    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			}
			Result result = htable.get(get);
			if (result.isEmpty()) {
				LOG.error("Row with the key "+ loc.getBytes() +" does not exists. Cannot perform update operation");
				return false;
			}

//			Put put = new Put(loc.getBytes());
//			for (int i = 0; i < row.length; i++) {
//				if (LOG.isTraceEnabled())
//					LOG.trace("update row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
//				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
//			}
//
//			if (transID != null)
//				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			Put put = Puts.buildUpdate(loc,row,validColumns,transID);
			//FIXME: checkAndPut can only do one column at a time, too expensive
			htable.put(put);
			return true;
		} catch (IOException ie) {
			LOG.error(ie.getMessage(), ie);
		} 
		return false;
	}

	public static String generateQuorum() {
		LOG.info("generateQuorum");
		String servers = config.get(HBaseConstants.HBASE_ZOOKEEPER_QUOROM, "localhost");
		String port = config.get(HBaseConstants.HBASE_ZOOKEEPER_CLIENT_PORT, "2181");
		StringBuilder sb = new StringBuilder();
		for (String split: servers.split(",")) {
			sb.append(split);
			sb.append(":");
			sb.append(port);
			sb.append(",");
		}
		sb.delete(sb.length() - 1, sb.length());
		return sb.toString();
	}

	public static long generateConglomSequence() {
		if (LOG.isTraceEnabled())
			LOG.trace("generateConglomSequence");
		try {
			String node = rzk.create(conglomeratePath + "/", new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			long test = Long.parseLong(node.substring(node.length() - 10, node.length()));
			if (LOG.isTraceEnabled())
				LOG.trace("conglom Sequence returned " +test);
			return test;
		} catch (KeeperException e) {
			//TODO -sf- better handling!
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		} catch (InterruptedException e) {
			//TODO -sf- better handling!
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}
		return (Long) null;
	}

	public static void splitConglomerate(long conglomId) throws IOException, InterruptedException {
		HBaseAdmin admin = new HBaseAdmin(config);

		byte[] name = SpliceAccessManager.getHTable(conglomId).getTableName();
		admin.split(name);
	}

	/**
	 * Synchronously splits a Conglomerate around the specified position.
	 *
	 * @param conglomId the conglom to split
	 * @param position the position to split around
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void splitConglomerate(long conglomId, byte[] position) throws IOException, InterruptedException {
		HBaseAdmin admin = new HBaseAdmin(config);

		byte[] name = SpliceAccessManager.getHTable(conglomId).getTableName();
		admin.split(name,position);

		boolean isSplitting=true;
		while(isSplitting){
			isSplitting=false;
			//synchronously wait here until no TableRegions report isSplit()
			List<HRegionInfo> regions = admin.getTableRegions(name);
			for(HRegionInfo region:regions){
				if(region.isSplit()){
					isSplitting=true;
					break;
				}
			}
			Thread.sleep(1000l);
		}
	}

	public static long getHighestConglomSequence(){
		SpliceLogUtils.trace(LOG, "getHighestConglomSequence");
		try{
			List<String> children = rzk.getChildren(conglomeratePath, false);
			//filter out elements with @ sign in them--those aren't conglom sequences 
			List<Long> values = Lists.newArrayList(Collections2.transform(Collections2.filter(children, new Predicate<String>(){
				@Override
				public boolean apply(String next){
					return next!=null&&!next.contains("@");
				}
			}),new Function<String,Long>(){
				@Override
				public Long apply(String source){
					return Long.valueOf(source);
				}
			}));
			Collections.sort(values);
			return values.get(values.size()-1);
		}catch(KeeperException e){
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		} catch (InterruptedException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		//can't ever happen,since catch blocks will throw runtime exceptions
		return -1l;
	}

	public static String generateQueryNodeSequence() {
		SpliceLogUtils.trace(LOG,"generateQueryNodeSequence");
		try {			
			String node = rzk.create(queryNodePath + "/", Bytes.toBytes(OperationTreeStatus.CREATED.toString()), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
			if (LOG.isTraceEnabled())
				LOG.trace("generate Query Node Sequence " +node);
			return node;
		} catch (KeeperException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		} catch (InterruptedException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
		}
		return null;
	}	

	public static void setQueryWaitNode(String uniqueSequenceID, Watcher watcher) {
		if (LOG.isTraceEnabled())
			LOG.trace("setQueryWaitNode");
		try {
			rzk.getData(uniqueSequenceID, watcher, null);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	

	
	
	public static boolean created() {
		if (LOG.isTraceEnabled())
			LOG.trace("started ");
		try {
			List<String> paths = rzk.getChildren(derbyPropertyPath, false);
			return paths != null && paths.size() > 5;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static boolean propertyExists(String propertyName) {
		if (LOG.isTraceEnabled())
			LOG.trace("propertyExists " + propertyName);
		try {
			return rzk.exists(derbyPropertyPath + "/" + propertyName, false) != null;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return false;
	}

	public static void addProperty(String propertyName, String propertyValue) {
		if (LOG.isTraceEnabled())
			LOG.trace("addProperty name " + propertyName + ", value "+ propertyValue);
		try {
			rzk.create(derbyPropertyPath + "/" + propertyName, Bytes.toBytes(propertyValue), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static String getProperty(String propertyName) {
		if (LOG.isTraceEnabled())
			LOG.trace("getProperty " + propertyName);
		try {
			byte[] data = rzk.getData(derbyPropertyPath + "/" + propertyName, false, null);
			if (LOG.isTraceEnabled())
				LOG.trace("getProperty name " + propertyName + ", value "+ Bytes.toString(data));
			return Bytes.toString(data);
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	public static Properties getAllProperties(Properties defaultProperties) {
		if (LOG.isTraceEnabled())
			LOG.trace("getAllProperties " + defaultProperties);
		Properties properties = new Properties(defaultProperties);
		try {
			List<String> keys = rzk.getChildren(derbyPropertyPath, false);
			for (String key : keys) {
				byte[] data = rzk.getData(derbyPropertyPath + "/" + key, false, null);
				if (LOG.isTraceEnabled())
					LOG.trace("getAllProperties retrieved property " + key + ", value " + Bytes.toString(data));
				properties.put(key, Bytes.toString(data));
			}
			return properties;
		} catch (KeeperException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return properties;
	}
	
	public static HTableDescriptor generateDefaultDescriptor(String tableName) {
		HTableDescriptor desc = new HTableDescriptor(tableName);
		desc.addFamily(new HColumnDescriptor(HBaseConstants.DEFAULT_FAMILY.getBytes(),
				HBaseConstants.DEFAULT_VERSIONS,
				HBaseConstants.DEFAULT_COMPRESSION,
				HBaseConstants.DEFAULT_IN_MEMORY,
				HBaseConstants.DEFAULT_BLOCKCACHE,
				HBaseConstants.DEFAULT_TTL,
				HBaseConstants.DEFAULT_BLOOMFILTER));
		return desc;		
	}

	public static String getTransIDString(Transaction trans) {
		if (trans == null)
			return null;
		
		//for debugging purpose right now
		if (!(trans instanceof ZookeeperTransaction))
			LOG.error("We should only support ZookeeperTransaction!");
		
		ZookeeperTransaction zt = (ZookeeperTransaction)trans;
		if (zt.getTransactionState() != null && zt.getTransactionState().getTransactionID() != null)
			return zt.getTransactionState().getTransactionID();
		
		return null;
	}
	
	public static byte[] getTransID(Transaction trans) {
		String transID = getTransIDString(trans);
		if (transID == null)
			return null;
		
		return transID.getBytes();
	}

	public static byte[] getUniqueKey(){
		return gen.next().toString().getBytes();			
	}

	public static byte[] generateInstructions(Activation activation,SpliceOperation topOperation) {
		/*
		 * Serialize out any non-null result rows that are currently stored in the activation.
		 *
		 * This is necessary if you are pushing out a set of Operation to a Table inside of a Sink.
		 */
		int rowPos=1;
		Map<Integer,ExecRow> rowMap = new HashMap<Integer,ExecRow>();
		boolean shouldContinue=true;
		while(shouldContinue){
			try{
				ExecRow row = (ExecRow)activation.getCurrentRow(rowPos);
				if(row!=null){
					rowMap.put(rowPos,row);
				}
				rowPos++;
			}catch(IndexOutOfBoundsException ie){
				//we've reached the end of the row group in activation, so stop
				shouldContinue=false;
			}
		}
		ExecRow[] currentRows = new ExecRow[rowPos];
		for(Integer rowPosition:rowMap.keySet()){
			ExecRow row = new SerializingExecRow(rowMap.get(rowPosition));
			if(row instanceof ExecIndexRow)
				currentRows[rowPosition] = new SerializingIndexRow(row);
			else
				currentRows[rowPosition] =  row;
		}
		SpliceLogUtils.trace(LOG,"serializing current rows: %s", Arrays.toString(currentRows));

		SpliceObserverInstructions instructions = new SpliceObserverInstructions(
																								(GenericStorablePreparedStatement) activation.getPreparedStatement(),
																								currentRows,
																								topOperation);
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(instructions);
			oos.flush();
			oos.close();
			return out.toByteArray();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error generating Splice instructions:"+e.getMessage(),e);
			return null;
		}
	}

	public static void setInstructions(Scan scan, Activation activation, SpliceOperation topOperation){
		scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(activation,topOperation));
	}

	public static void setThreadContext(){
		SpliceLogUtils.trace(LOG,"addThreadContext");
		ContextService contextService = new ContextService();
		ContextManager mgr = contextService.newContextManager();
		mgr.pushContext(SpliceEngine.getLanguageConnectionContext());
		contextService.setCurrentContextManager(mgr);
	}

}
