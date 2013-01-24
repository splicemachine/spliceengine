package com.splicemachine.derby.utils;

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
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.dictionary.TableDescriptor;
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
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.datanucleus.store.valuegenerator.UUIDHexGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * We need to get table name from database using Container ID and the info is stored in Data Dictionary.
 * FIXME: need to figure out how to get the info out
 * @author jessiezhang
 *
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
		createHTable(Long.toString(tableName),SpliceUtils.toJSON(conglomerate));
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

	//Please do not remove the methods even if there are not used now until we finish the impl. I am planning to use them. 
	//I removed some of my methods since similar methods have been added to another class today - 10/23 
	public String getColumnName(TableDescriptor td, int position) {
		String[] columnNames = td.getColumnNamesArray();
		return columnNames[position];
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
		if(scanList==null) populate(currentResult,destRow);
		else{
			try{
				for(int i=scanList.anySetBit();i!=-1;i=scanList.anySetBit(i)){
					byte[] value = currentResult.getValue(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(i).getBytes());
					fill(value,destRow[bitSetToDestRowMap[i]]);
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

	public static Put insert(DataValueDescriptor[] row, byte[] transID) throws StandardException {
		return insert(row,gen.next().toString().getBytes(), transID);
	}

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
	public static Put insert(DataValueDescriptor[] row, byte[] key, byte[] transID, DataValueDescriptor[] additionalInserts) throws StandardException {
		Put put = insert(row,key,transID);
		put = insertAdditional(additionalInserts,put);
		return put;
	}
	
	public static Put insertAdditional(DataValueDescriptor[] row, Put put) throws StandardException {
//		DerbyLogUtils.traceDescriptors(LOG, "insert row with key: "+ key +" and row", row);
		try {
			for (int i = 0; i < row.length; i++) {
				if (row[i] != null && !row[i].isNull()) {
//					SpliceLogUtils.trace(LOG, "insert row type %s, value %s, qualifier %d",row[i].getTypeName(),row[i].getTraceString(),i);
					put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(-(i+1))).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
				} else {
					SpliceLogUtils.trace(LOG,
							"skipping row since it was null "+ (row[i]==null? "NULL":row[i].getTypeName()) + 
							", value "+ (row[i]==null? "NULL" : row[i].getTraceString()));
				}
			}
			return put;
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	
	public static Put insert(DataValueDescriptor[] row, byte[] key, byte[] transID) throws StandardException {
//		DerbyLogUtils.traceDescriptors(LOG, "insert row with key: "+ key +" and row", row);
		try {
			Put put = new Put(key);
			for (int i = 0; i < row.length; i++) {
				if (row[i] != null && !row[i].isNull()) {
//					SpliceLogUtils.trace(LOG, "insert row type %s, value %s, qualifier %d",row[i].getTypeName(),row[i].getTraceString(),i);
					put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
				} else {
					SpliceLogUtils.trace(LOG,
							"skipping row since it was null "+ (row[i]==null? "NULL":row[i].getTypeName()) + 
							", value "+ (row[i]==null? "NULL" : row[i].getTraceString()));
				}

			}
			if (transID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			return put;
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	

	public static Put insert(DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] transID) throws StandardException {
		return insert(row,validColumns,gen.next().toString().getBytes(), transID);
	}

	public static Put insert(DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] key, byte[] transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("insert row " + row + " with key " + key+" with validColumns "+validColumns);
		try {
			Put put = new Put(key);
			int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
			for (int i = 0; i < numrows; i++) { 
				if ( (validColumns != null && !validColumns.isSet(i)) || row[i] == null || row[i].isNull()) 
					continue;

				if (LOG.isTraceEnabled())
					LOG.trace("insert row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));			
			}
			if (transID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			return put;
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

	public static Put update(RowLocation rowLocation, DataValueDescriptor[] row, FormatableBitSet validColumns, byte[] transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("insert row " + rowLocation);

		try {
			Put put = new Put(rowLocation.getBytes());
			int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
			for (int i = 0; i < numrows; i++) {
				if ((validColumns != null && !validColumns.isSet(i)) || row[i] == null || row[i].isNull())
					continue;

				if (LOG.isTraceEnabled())
					LOG.trace("update row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));			
			}
			if (transID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			return put;
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}

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

			Put put = new Put(loc.getBytes());
			for (int i = 0; i < row.length; i++) {
				if (LOG.isTraceEnabled())
					LOG.trace("update row type " + row[i].getTypeName() + ", value " + row[i].getTraceString());
				put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), DerbyBytesUtil.generateBytes(row[i]));
			}
			
			if (transID != null)
				put.setAttribute(TxnConstants.TRANSACTION_ID, transID);
			//FIXME: checkAndPut can only do one column at a time, too expensive
			htable.put(put);
			return true;
		} catch (IOException ie) {
			LOG.error(ie.getMessage(), ie);
		} 
		return false;
	}

//	public static CompareFilter.CompareOp getHBaseCompareOp(int derbyOperator, boolean negateCompareResult) {
//		CompareFilter.CompareOp op = null;
//
//		if (negateCompareResult) {
//			switch (derbyOperator)
//			{
//			case DataValueDescriptor.ORDER_OP_EQUALS:
//				op = CompareFilter.CompareOp.NOT_EQUAL;
//				break;
//			case DataValueDescriptor.ORDER_OP_LESSTHAN:
//				op = CompareFilter.CompareOp.GREATER_OR_EQUAL;
//				break;
//			case DataValueDescriptor.ORDER_OP_GREATERTHAN:
//				op = CompareFilter.CompareOp.LESS_OR_EQUAL;
//				break;
//			case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
//				op = CompareFilter.CompareOp.GREATER;
//				break;
//			case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
//				op = CompareFilter.CompareOp.LESS;
//				break;
//			default:
//				throw new RuntimeException("Derby Operator " + derbyOperator);
//			}
//		} else {
//			switch (derbyOperator)
//			{
//			case DataValueDescriptor.ORDER_OP_EQUALS:
//				op = CompareFilter.CompareOp.EQUAL;
//				break;
//			case DataValueDescriptor.ORDER_OP_LESSTHAN:
//				op = CompareFilter.CompareOp.LESS;
//				break;
//			case DataValueDescriptor.ORDER_OP_GREATERTHAN:
//				op = CompareFilter.CompareOp.GREATER;
//				break;
//			case DataValueDescriptor.ORDER_OP_LESSOREQUALS:
//				op = CompareFilter.CompareOp.LESS_OR_EQUAL;
//				break;
//			case DataValueDescriptor.ORDER_OP_GREATEROREQUALS:
//				op = CompareFilter.CompareOp.GREATER_OR_EQUAL;
//				break;
//			default:
//				throw new RuntimeException("Derby Operator " + derbyOperator);
//			}
//		}
//		return op;
//	}

//    public static FilterList buildFilter(Qualifier[] qualifiers, Operator operator) throws StandardException, IOException {
//        FilterList list = new FilterList(operator);
//        for(Qualifier qualifier:qualifiers){
//            DataValueDescriptor dvd = qualifier.getOrderable();
//            if(dvd==null||dvd.isNull()||dvd.isNullOp().getBoolean()){
//                CompareFilter.CompareOp op = qualifier.negateCompareResult()? CompareFilter.CompareOp.NOT_EQUAL :
//                                                                                CompareFilter.CompareOp.EQUAL;
//                list.addFilter(new ColumnNullableFilter(HBaseConstants.DEFAULT_FAMILY_BYTES,
//                                                        Integer.toString(qualifier.getColumnId()).getBytes(),
//                                                        op));
//            }else{
//                SingleColumnValueFilter filter = new SingleColumnValueFilter(HBaseConstants.DEFAULT_FAMILY_BYTES,
//                        Integer.toString(qualifier.getColumnId()).getBytes(),
//                        getHBaseCompareOp(qualifier.getOperator(),qualifier.negateCompareResult()),
//                        DerbyBytesUtil.generateBytes(dvd));
//                filter.setFilterIfMissing(true);
//                list.addFilter(filter);
//            }
//        }
//        return list;
//    }

//    public static Filter constructFilter(Qualifier[][] qualList) throws StandardException,IOException{
//        //build and clauses from first row of qualifiers
//        //SpliceLogUtils.trace(LOG, "constructing filter from qualList %s", SpliceLogUtils.stringify(qualList));
//        FilterList finalFilter = new FilterList(Operator.MUST_PASS_ALL);
//
//        SpliceLogUtils.trace(LOG,"building and filters");
//        finalFilter.addFilter(buildFilter(qualList[0],Operator.MUST_PASS_ALL));
//
//        //build or clauses from the rest
//        SpliceLogUtils.trace(LOG,"building or filters");
//        FilterList orList = new FilterList(Operator.MUST_PASS_ALL);
//        for(int clause=1;clause<qualList.length;clause++){
//            orList.addFilter(buildFilter(qualList[clause],Operator.MUST_PASS_ONE));
//        }
//        finalFilter.addFilter(orList);
//
//        return finalFilter;
//    }

//	public static Filter generateFilter(Qualifier[][] qual_list) throws StandardException, IOException {
//		SpliceLogUtils.trace(LOG, "generateFilter " + qual_list);
//		FilterList masterList = new FilterList(Operator.MUST_PASS_ALL);
//		FilterList andList = new FilterList(Operator.MUST_PASS_ALL);
//		SpliceLogUtils.trace(LOG, "Generating where clause %d", qual_list[0].length);
//		for (int i = 0; i < qual_list[0].length; i++) { // AND CLAUSES
//			Qualifier q = qual_list[0][i];
//			DataValueDescriptor o = q.getOrderable();
//			if(o!=null)
//				SpliceLogUtils.trace(LOG," and column=%d, operator=%s, nullable=%s,q.getOrderable().isNullOp().getBoolean()=%b,q.negateCompareResult()=%b,%s %s",
//									q.getColumnId(),q.getOperator(),q.getOrderedNulls(),
//									o.isNullOp().getBoolean(),q.negateCompareResult(),o.getTraceString(),q.getClass());
//			else
//				SpliceLogUtils.trace(LOG,"q = %s",q);
//			if (q.getOrderable()==null||q.getOrderable().isNullOp().getBoolean()) {
//				SpliceLogUtils.trace(LOG,"orderable is null");
//				if (q.negateCompareResult()) { //IS NOT NULL
//					SpliceLogUtils.trace(LOG, "negateCompareResult");
//					andList.addFilter(new ColumnNullableFilter(
//							HBaseConstants.DEFAULT_FAMILY.getBytes(),
//							Integer.toString(q.getColumnId()).getBytes(),
//							CompareFilter.CompareOp.NOT_EQUAL));
//				} else { //IS NULL
//					SpliceLogUtils.trace(LOG, "compareResult");
//					andList.addFilter(new ColumnNullableFilter(
//							HBaseConstants.DEFAULT_FAMILY.getBytes(),
//							Integer.toString(q.getColumnId()).getBytes(),
//							CompareFilter.CompareOp.EQUAL));
//				}
//			} else {
//				SpliceLogUtils.trace(LOG, "orderable is not null");
//				SingleColumnValueFilter filter = new SingleColumnValueFilter(
//						HBaseConstants.DEFAULT_FAMILY.getBytes(),
//						Integer.toString(q.getColumnId()).getBytes(),
//						getHBaseCompareOp(q.getOperator(), q.negateCompareResult()),
//						DerbyBytesUtil.generateBytes(q.getOrderable()));
//				filter.setFilterIfMissing(true);
//				andList.addFilter(filter);
//			}
//		}
//		SpliceLogUtils.trace(LOG, "adding and filters");
//		masterList.addFilter(andList);
//		FilterList nextMasterList = new FilterList(Operator.MUST_PASS_ALL);
//		SpliceLogUtils.trace(LOG,"and_idx.length="+qual_list.length);
//		for (int and_idx = 1; and_idx < qual_list.length; and_idx++) { // OR CLAUSES
//			SpliceLogUtils.trace(LOG,"or branch");
//			// loop through each of the "and" clause.
//			FilterList orList = new FilterList(Operator.MUST_PASS_ONE);
//
//			SpliceLogUtils.trace(LOG, "qual_list[and_idx].length=%d",qual_list[and_idx].length);
//			for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++) {
//				Qualifier q = qual_list[and_idx][or_idx];
//				SpliceLogUtils.trace(LOG,"q=%s",q);
//				if (LOG.isTraceEnabled()&&q.getOrderable()!=null)
//					LOG.trace(" or column " + q.getColumnId() +" "+ q.getOperator() + q.getOrderable().getTraceString());
//				else if(LOG.isTraceEnabled())
//					LOG.trace(" or column " + q.getColumnId() +" "+ q.getOperator());
//
//				if (q.getOrderable()==null || q.getOrderable().isNullOp().getBoolean()) {
//					if (q.negateCompareResult()) { //IS NOT NULL
//						orList.addFilter(new ColumnNullableFilter(
//								HBaseConstants.DEFAULT_FAMILY.getBytes(),
//								Integer.toString(q.getColumnId()).getBytes(),
//								CompareFilter.CompareOp.NOT_EQUAL));
//
//					} else { //IS NULL
//						orList.addFilter(new ColumnNullableFilter(
//								HBaseConstants.DEFAULT_FAMILY.getBytes(),
//								Integer.toString(q.getColumnId()).getBytes(),
//								CompareFilter.CompareOp.EQUAL));
//					}
//				} else {
//					SingleColumnValueFilter filter2 = new SingleColumnValueFilter(
//							HBaseConstants.DEFAULT_FAMILY.getBytes(),
//							Integer.toString(q.getColumnId()).getBytes(),
//							getHBaseCompareOp(q.getOperator(), q.negateCompareResult()),
//							DerbyBytesUtil.generateBytes(q.getOrderable()));
//							filter2.setFilterIfMissing(true);
//					orList.addFilter(filter2);
//				}
//			}
//			nextMasterList.addFilter(orList);
//		}
//		masterList.addFilter(nextMasterList);
//		return masterList;
//	}

	/*
	 * This is a short term workaround
	 * @param qual_list
	 * @return
	 * @throws StandardException
	 * @throws IOException
	 */
//	public static Filter generateIndexFilter(DataValueDescriptor[] dataValueDescriptors, int operator) throws StandardException, IOException {
//        SpliceLogUtils.trace(LOG,"Generating Index Filters %s with operator %d",Arrays.toString(dataValueDescriptors),operator);
//		FilterList masterList = new FilterList(Operator.MUST_PASS_ALL);
//        SpliceLogUtils.trace(LOG,"Generating where clause %d",dataValueDescriptors.length);
//		for (int i = 0; i < dataValueDescriptors.length; i++) { // AND CLAUSES
//            DataValueDescriptor dvd = dataValueDescriptors[i];
//			SpliceLogUtils.trace(LOG,"dvd=%s",dvd);
//			if (dvd !=null && !dvd.isNull()){
//				SpliceLogUtils.trace(LOG,"adding filter %s",dvd);
//				masterList.addFilter(new SingleColumnValueFilter(HBaseConstants.DEFAULT_FAMILY.getBytes(),
//						(new Integer(i)).toString().getBytes(),
//						getHBaseCompareOp(operator, false),
//						DerbyBytesUtil.generateBytes(dataValueDescriptors[i])));
//				SpliceLogUtils.trace(LOG, "successfully added filter %s",dataValueDescriptors[i].getObject());
//			}else
//				SpliceLogUtils.trace(LOG,"skipping dataValueDescriptor %s because object is null",dataValueDescriptors[i]);
//		}
//		return masterList;
//	}

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
	
//	public static Scan generateScan(DataValueDescriptor uniqueString, byte[] startRow, byte[] stopRow, String transactionID) throws StandardException, IOException {
//		return generateScan(uniqueString, startRow, stopRow, transactionID.getBytes(), 100);
//	}
	
//	public static Scan generateScan(DataValueDescriptor uniqueString, byte[] startRow, byte[] stopRow, byte[] transactionID) throws StandardException, IOException {
//		return generateScan(uniqueString, startRow, stopRow, transactionID, 100);
//	}
//
//	public static Scan generateScan(DataValueDescriptor uniqueString, byte[] startRow, byte[] stopRow, String transactionID, int caching) throws StandardException, IOException {
//		return generateScan(uniqueString, startRow, stopRow, transactionID.getBytes(), caching);
//	}
	
//	public static Scan generateScan(DataValueDescriptor uniqueString, byte[] startRow, byte[] stopRow, byte[] transactionID, int caching) throws StandardException, IOException {
//		SpliceLogUtils.trace(LOG,"generateRegionScan for uniqueString %s",uniqueString);
//		Scan scan = new Scan();
//		scan.setCaching(caching);
//		scan.setStartRow(startRow);
//		scan.setStopRow(stopRow);
//		scan.addFamily(HBaseConstants.DEFAULT_FAMILY_BYTES);
//		if (transactionID != null) { //no transaction, no need for isolation level
//			scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionID);
//			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
//					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
//		}
//		return scan;
//	}
	
//	public static Scan setupScan(byte[] transactionID,FormatableBitSet scanColumnList, Qualifier[][] qualifier,
//			DataValueDescriptor[] startKeyValue,
//			int startSearchOperator,
//			DataValueDescriptor[] stopKeyValue,
//			int stopSearchOperator,
//			boolean[] ascDescInfo) {
//			SpliceLogUtils.trace(LOG,"setting up scan");
//			Scan scan = setupScanKeys(transactionID,scanColumnList, qualifier,startKeyValue,startSearchOperator,stopKeyValue,stopSearchOperator,ascDescInfo);
//            SpliceLogUtils.trace(LOG,"Attaching filters to scan");
////            return scan;
//			return attachFilterToScan(scan,qualifier,startKeyValue,startSearchOperator,stopKeyValue,stopSearchOperator);
//	}
	
//	public static Scan setupScanKeys(byte[] transactionID,FormatableBitSet scanColumnList, Qualifier[][] qualifier,
//			DataValueDescriptor[] startKeyValue,
//			int startSearchOperator,
//			DataValueDescriptor[] stopKeyValue,
//			int stopSearchOperator,
//			boolean[] ascDescInfo) {
//        SpliceLogUtils.trace(LOG,"setupScanKeys");
//		Scan scan = new Scan();
//		scan.setCaching(100);
//		if (transactionID != null) {
//			scan.setAttribute(TxnConstants.TRANSACTION_ID, transactionID);
//			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
//					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
//		} else {
//			scan.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
//					Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_COMMITTED.toString()));
//		}
//
//		FormatableBitSet tempScanColumnList = scanColumnList;
//		if (scanColumnList != null && qualifier != null && qualifier.length > 0) {
//			for (int and_idx = 0; and_idx < qualifier.length; and_idx++) {
//				for (int or_idx = 0; or_idx < qualifier[and_idx].length; or_idx++) {
//					if (!scanColumnList.isSet(qualifier[and_idx][or_idx].getColumnId()))
//						scanColumnList.set(qualifier[and_idx][or_idx].getColumnId());
//				}
//			}
//		}
//		if (tempScanColumnList != null) {
//			for (int i = 0; i < tempScanColumnList.size(); i++) {
//				if (tempScanColumnList.isSet(i))
//					scan.addColumn(HBaseConstants.DEFAULT_FAMILY.getBytes(), Integer.toString(i).getBytes());
//			}
//		}
//		try {
//			boolean generateKey = true;
//			if (startKeyValue != null && stopKeyValue != null) {
//				for (int i =0; i<startKeyValue.length; i++) {
//					if (startKeyValue[i]==null||startKeyValue[i].isNull())
//						generateKey = false; // DO WE NEED THIS? JL
//				}
//			}
//			if (generateKey) {
//				boolean[] sortOrder = ascDescInfo;
//				scan.setStartRow(DerbyBytesUtil.generateScanKeyForIndex(startKeyValue,startSearchOperator,sortOrder));
//				scan.setStopRow(DerbyBytesUtil.generateScanKeyForIndex(stopKeyValue,stopSearchOperator,sortOrder));
//				if (scan.getStartRow() != null && scan.getStopRow() != null && Bytes.compareTo(scan.getStartRow(), scan.getStopRow())>=0) {
//					LOG.warn("Scan begin key is greater than the end key");
//				}
//			}
//            //if we can't fill the start row somehow, assume that it's empty -SF-
//            if (scan.getStartRow() == null)
//                scan.setStartRow(HConstants.EMPTY_START_ROW);
//            if (scan.getStopRow() == null)
//                scan.setStopRow(HConstants.EMPTY_END_ROW);
//		} catch (Exception e) {
//			LOG.error("Exception creating start key",e);
//			throw new RuntimeException(e);
//		}
//		return scan;
//	}
	
//	public static Scan attachFilterToScan(Scan scan, Qualifier[][] qualifier,DataValueDescriptor[] startKeyValue,
//			int startSearchOperator,
//			DataValueDescriptor[] stopKeyValue,
//			int stopSearchOperator)  {
//            SpliceLogUtils.trace(LOG,"attachFilterToScan");
//			try {
//				FilterList masterList = new FilterList(Operator.MUST_PASS_ALL);
//				if (qualifier != null){
//                    SpliceLogUtils.trace(LOG,"Attaching qualifier filter to scan");
//					masterList.addFilter(SpliceUtils.constructFilter(qualifier));
//                }if (startSearchOperator == 1 && stopSearchOperator == 1
//                                             && startKeyValue != null
//                                             && startKeyValue.length == 1
//                                             &&startKeyValue[0].isNull()){
//					LOG.info("NOT GENERATING INDEX FILTER. DO A FULL SCAN.......");
//				} else {
//                    /*
//                     * You have to set a start equals filter, otherwise a restricted query won't work
//                     * correctly--e.g. "select * from table t where t.a = SYS" would pull back
//                     * t.a = SYS
//                     * t.a = SYSCAT
//                     * t.a = SYSTABLE
//                     * ...
//                     *
//                     * when it should really pull back just "t.a = SYS"
//                     */
//					if (startKeyValue != null && startSearchOperator >= 0){
//                        SpliceLogUtils.trace(LOG,"Attaching start index filter to scan. " +
//                                                 "startSearchOperator=%d,startKeyValue=%s",
//                                                    startSearchOperator,Arrays.toString(startKeyValue));
//						masterList.addFilter(SpliceUtils.generateIndexFilter(startKeyValue, Orderable.ORDER_OP_EQUALS));
//                    }
////                    if (stopKeyValue != null && stopSearchOperator >= 0){
////                        SpliceLogUtils.trace(LOG,"Attaching stop index filter to scan. " +
////                                "stopSearchOperator=%d,stopKeyValue=%s",
////                                stopSearchOperator,Arrays.toString(stopKeyValue));
////						masterList.addFilter(SpliceUtils.generateIndexFilter(stopKeyValue,stopSearchOperator));
////                    }
//				}
//			    scan.setFilter(masterList);
//			} catch (Exception e) {
//				throw new RuntimeException("error attaching Filter",e);
//			}
//			return scan;
//	}

	public static byte[] getUniqueKey(){
		return gen.next().toString().getBytes();			
	}

	public static byte[] generateInstructions(Activation activation,SpliceOperation topOperation) {
		SpliceObserverInstructions instructions = new SpliceObserverInstructions((GenericStorablePreparedStatement) activation.getPreparedStatement(),topOperation);
		try {
			ByteArrayOutputStream out = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(out);
			oos.writeObject(instructions);
            oos.flush();
            oos.close();
			return out.toByteArray();
		} catch (IOException e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Error creating SpliceNoPutResultSet "+e.getMessage(),e);
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
