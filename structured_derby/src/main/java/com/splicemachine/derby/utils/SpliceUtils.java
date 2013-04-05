package com.splicemachine.derby.utils;

import com.google.gson.Gson;
import com.splicemachine.SpliceConfiguration;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.constants.ITransactionGetsPuts;
import com.splicemachine.constants.TxnConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.sql.execute.operations.OperationTree.OperationTreeStatus;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.hbase.txn.ZkTransactionGetsPuts;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import com.splicemachine.si2.si.api.ClientTransactor;
import com.splicemachine.si2.txn.SiGetsPuts;
import com.splicemachine.si2.txn.TransactionTableCreator;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.context.ContextManager;
import org.apache.derby.iapi.services.context.ContextService;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.conn.LanguageConnectionContext;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
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

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Utility methods
 * @author jessiezhang
 * @author johnleach
 * @author scottfines
 */

@SuppressWarnings(value = "deprecation")
public class SpliceUtils {
	private static Logger LOG = Logger.getLogger(SpliceUtils.class);

    public static final boolean useSi = true;
    public static final String NA_TRANSACTION_ID = "NA_TRANSACTION_ID";
    private static final String SI_EXEMPT = "si-exempt";

    /**
     * Populates an array of DataValueDescriptors with a default value based on their type.
     *
     * This is used mainly to prevent NullPointerExceptions from occurring in administrative
     * operations such as getExecRowDefinition().
     *
     * @param dvds
     * @throws StandardException
     */
    public static void populateDefaultValues(DataValueDescriptor[] dvds) throws StandardException {
        for(DataValueDescriptor dvd:dvds){
            if(dvd.isNull()){
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_DOUBLE_ID:
                        dvd.setValue(0.0d);
                        break;
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                        dvd.setValue(0);
                        break;
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        dvd.setValue(false);
                        break;
                    case StoredFormatIds.SQL_LONGINT_ID:
                        dvd.setValue(0l);
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.setValue(0f);
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }

    public static Scan createScan(String transactionId) {
        try {
            return (Scan) attachTransaction(new Scan(), transactionId);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
        }
        // can't reach this point
        throw new RuntimeException("Cannot reach this state.");
    }

    public enum SpliceConglomerate {HEAP,BTREE}
	public static Configuration config = SpliceConfiguration.create();
	protected static Gson gson = new Gson();
	protected static UUIDHexGenerator gen = new UUIDHexGenerator("Splice", null);
	protected static String quorum;
	protected static String transPath;
	protected static String derbyPropertyPath = "/derbyPropertyPath";
	protected static String queryNodePath = "/queryNodePath";
	protected static RecoverableZooKeeper rzk = null;
	protected static ZooKeeperWatcher zkw = null;

    protected static HConnection connection;
	static {
		quorum = generateQuorum();
//		conglomeratePath = config.get(SchemaConstants.CONGLOMERATE_PATH_NAME,SchemaConstants.DEFAULT_CONGLOMERATE_SCHEMA_PATH);
		transPath = config.get(TxnConstants.TRANSACTION_PATH_NAME,TxnConstants.DEFAULT_TRANSACTION_PATH);
		try {

			zkw = (new HBaseAdmin(config)).getConnection().getZooKeeperWatcher();
			rzk = zkw.getRecoverableZooKeeper();

			if (rzk.exists(queryNodePath, false) == null)
				rzk.create(queryNodePath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (rzk.exists(derbyPropertyPath, false) == null)
				rzk.create(derbyPropertyPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
			if (rzk.exists(transPath, false) == null)
				rzk.create(transPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);

            connection = HConnectionManager.getConnection(config);
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {

		}
	}

	public static HBaseAdmin getAdmin() {
		try {
			return new HBaseAdmin(config);
		} catch (MasterNotRunningException e) {
			throw new RuntimeException(e);
		} catch (ZooKeeperConnectionException e) {
			throw new RuntimeException(e);
		}
	}

	public static String getTransactionPath() {
		return transPath;
	}


	public static String toJSON(Object object) {
		return gson.toJson(object);
	}

	public static <T> T fromJSON(String json,Class<T> instanceClass) {
		return gson.fromJson(json, instanceClass);
	}

    public static Get createGet(Mutation mutation, byte[] row) throws IOException {
        return createGet(getTransactionId(mutation), row);
    }

    public static Get createGet(String transactionId, byte[] row) throws IOException {
        return (Get) attachTransaction(new Get(row), transactionId);
    }

    public static Get createGet(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, String transID) throws StandardException {
		SpliceLogUtils.trace(LOG,"createGet %s",loc.getBytes());
		try {
			Get get = createGet(transID, loc.getBytes());
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
				get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
													Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			}
			return get;
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG,e);
			return null;
		}
	}

    /**
     * Perform a Delete against a table. The operation which is actually performed depends on the transactional semantics.
     *
     * @param table the table to delete from
     * @param transactionId the transaction to delete under
     * @param row the row to delete.
     * @throws IOException if something goes wrong during deletion.
     */
    public static void doDelete(HTableInterface table, String transactionId, byte[] row) throws IOException {
        Mutation mutation = Mutations.getDeleteOp(transactionId,row);
        if(mutation instanceof Put)
            table.put((Put)mutation);
        else
            table.delete((Delete)mutation);
    }

    public static Put createPut(byte[] newRowKey, Mutation mutation) throws IOException {
        return createPut( newRowKey, getTransactionId(mutation));
    }

    public static Put createPut(byte[] newRowKey, String transactionID) throws IOException {
        return (Put) attachTransaction(new Put(newRowKey), transactionID);
    }

    /**
     * Attach transactional information to the specified operation.
     *
     * @param op the operation to attach to.
     * @param txnId the transaction id to attach.
     */
    public static OperationWithAttributes attachTransaction(OperationWithAttributes op, String txnId) throws IOException {
        if (txnId == null) {
            throw new RuntimeException("Cannot create operation with a null transactionId");
        }
        if (txnId.equals(NA_TRANSACTION_ID)) {
            op.setAttribute(SI_EXEMPT, Bytes.toBytes(true));
        } else {
            if (op instanceof Get)
                getTransactionGetsPuts().prepGet(txnId, (Get) op);
            else if (op instanceof Put)
                getTransactionGetsPuts().prepPut(txnId, (Put) op);
            else if (op instanceof Delete)
                getTransactionGetsPuts().prepDelete(txnId, (Delete) op);
            else
                getTransactionGetsPuts().prepScan(txnId, (Scan) op);
        }
        return op;
    }

    public static Delete createDelete(String transactionId, byte[] row) throws IOException {
        return (Delete) attachTransaction(new Delete(row), transactionId);
    }

    public static Put createDeletePut(Mutation mutation, byte[] rowKey) {
        return createDeletePut(getTransactionId(mutation), rowKey);
    }

    public static Put createDeletePut(String transactionId, byte[] rowKey) {
        final ClientTransactor clientTransactor = TransactorFactory.getDefaultClientTransactor();
        return (Put) clientTransactor.newDeletePut(clientTransactor.transactionIdFromString(transactionId), rowKey);
    }

    public static boolean isDelete(Mutation mutation) {
        if(mutation instanceof Delete) {
            return true;
        } else if (useSi) {
            return TransactorFactory.getDefaultClientTransactor().isDeletePut(mutation);
        } else {
            return false;
        }
    }

    /**
     * Get the transaction information from the specified mutation.
     *
     * @param mutation the mutation to get transaction information from
     * @return the transaction id specified by the given mutation.
     */
    public static String getTransactionId(Mutation mutation) {
        final byte[] exempt = mutation.getAttribute(SI_EXEMPT);
        if (exempt != null && Bytes.toBoolean(exempt)) {
            return NA_TRANSACTION_ID;
        }
        if(mutation instanceof Put)
            return getTransactionGetsPuts().getTransactionIdForPut((Put)mutation);
        else
            return getTransactionGetsPuts().getTransactionIdForDelete((Delete)mutation);
    }

    public static void handleNullsInUpdate(Put put, DataValueDescriptor[] row, FormatableBitSet validColumns) {
        if (validColumns != null) {
            int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
            for (int i = 0; i < numrows; i++) {
                if (validColumns.isSet(i) && row[i] != null && row[i].isNull())
                    put.add(HBaseConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), 0, SIConstants.EMPTY_BYTE_ARRAY);
            }
        }
    }

    public static void doCleanupNullsDelete(HTableInterface table, RowLocation loc,DataValueDescriptor[] destRow,
                                              FormatableBitSet validColumns, String transID)
            throws StandardException, IOException {
        if (useSi) {
            // handle this in the put
        } else {
            table.delete(cleanupNullsDelete(loc, destRow, validColumns, transID));
        }
    }

    private static Delete cleanupNullsDelete(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns,
                                             String transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("cleanupNullsDelete row ");
		try {
            Delete delete = Mutations.createDelete(transID, loc.getBytes());
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
		/**
		 * We have to use dataMap here instead of using currentResult.getValue() because for some reason columns larger
		 * than 9 will go missing if you call getValue() --likely its due to the fact that we are serializing ints
		 * as strings instead of as ints themselves.
		 */
		Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
		try{
			for(int i=0;i<destRow.length;i++){
				byte[] value = dataMap.get(Integer.toString(i).getBytes());
				fill(value,destRow[i]);
			}
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
		}
	}

    public static void populate(Result currentResult, DataValueDescriptor[] destRow,Serializer serializer) throws StandardException {
        SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",currentResult.raw().length,destRow.length);
        /**
         * We have to use dataMap here instead of using currentResult.getValue() because for some reason columns larger
         * than 9 will go missing if you call getValue() --likely its due to the fact that we are serializing ints
         * as strings instead of as ints themselves.
         */
        Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
        try{
            for(int i=0;i<destRow.length;i++){
                byte[] value = dataMap.get(Integer.toString(i).getBytes());
                fill(value,destRow[i],serializer);
            }
        }catch(IOException ioe){
            SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
        }
    }

    public static void populate(Result currentResult, FormatableBitSet scanColumnList, DataValueDescriptor[] destRow,Serializer serializer) throws StandardException {
        SpliceLogUtils.trace(LOG,"populate current Result %s using scanColumnList %s and destRow with size %d",currentResult,scanColumnList,destRow.length);
        try {
            if(scanColumnList == null) populate(currentResult,destRow,serializer);
            else{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
                for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
                    byte[] value = dataMap.get(Integer.toString(i).getBytes());
                    fill(value,destRow[i],serializer);
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error occurred during populate", e);
        }
    }

    public static void populate(Result currentResult, FormatableBitSet scanColumnList, DataValueDescriptor[] destRow) throws StandardException {
        SpliceLogUtils.trace(LOG,"populate current Result %s using scanColumnList %s and destRow with size %d",currentResult,scanColumnList,destRow.length);
        try {
            if(scanColumnList == null) populate(currentResult,destRow);
            else{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
                for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
                    byte[] value = dataMap.get(Integer.toString(i).getBytes());
                    fill(value,destRow[i]);
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error occurred during populate", e);
        }
    }

    public static void populate(Result currentResult, DataValueDescriptor[] destRow,
                                FormatableBitSet scanList,int[] bitSetToDestRowMap,Serializer serializer) throws StandardException{
        if(scanList==null||scanList.getNumBitsSet()<=0) populate(currentResult,destRow,serializer);
        else{
            try{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
                if (dataMap != null) {
                    for (int i = scanList.anySetBit(); i != -1; i = scanList.anySetBit(i)) {
                        byte[] value = dataMap.get(Integer.toString(i).getBytes());
                        SpliceLogUtils.trace(LOG, "Attempting to place column[%d] into destRow %s", i, destRow[bitSetToDestRowMap[i]]);
                        fill(value, destRow[bitSetToDestRowMap[i]], serializer);
                    }
                }
            }catch(IOException e){
                SpliceLogUtils.logAndThrowRuntime(LOG,"Error occurred during populate",e);
            }
        }
    }



    public static void populate(Result currentResult, DataValueDescriptor[] destRow,
															FormatableBitSet scanList,int[] bitSetToDestRowMap) throws StandardException{
		if(scanList==null||scanList.getNumBitsSet()<=0) populate(currentResult,destRow);
		else{
			try{
				Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(HBaseConstants.DEFAULT_FAMILY_BYTES);
				for(int i=scanList.anySetBit();i!=-1;i=scanList.anySetBit(i)){
					byte[] value = dataMap.get(Integer.toString(i).getBytes());
					SpliceLogUtils.trace(LOG,"Attempting to place column[%d] into destRow %s",i,destRow[bitSetToDestRowMap[i]]);
					fill(value, destRow[bitSetToDestRowMap[i]]);
				}
			}catch(IOException e){
				SpliceLogUtils.logAndThrowRuntime(LOG,"Error occurred during populate",e);
			}
		}
	}

	public static SpliceObserverInstructions getSpliceObserverInstructions(Scan scan) {
		byte[] instructions = scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS);
		if(instructions==null) return null;
		//Putting this here to prevent some kind of weird NullPointer situation
		//where the LanguageConnectionContext doesn't get initialized properly
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(instructions);
			ObjectInputStream ois = new ObjectInputStream(bis);
			SpliceObserverInstructions soi = (SpliceObserverInstructions) ois.readObject();
			return soi;
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
		}
		return null;
	}

    private static void fill(byte[] value, DataValueDescriptor descriptor, Serializer serializer) throws IOException, StandardException {
        if(value!=null&&descriptor!=null){
           serializer.deserialize(value,descriptor);
        }else if(descriptor!=null)descriptor.setToNull();
    }

	private static void fill(byte[] value, DataValueDescriptor dvd) throws StandardException, IOException {
		if(value!=null&&dvd!=null){
			DerbyBytesUtil.fromBytes(value,dvd);
		}else if(dvd!=null){
			dvd.setToNull();
		}
	}

	public static boolean update(RowLocation loc, DataValueDescriptor[] row,
			FormatableBitSet validColumns, HTableInterface htable, String transID) throws StandardException {
		if (LOG.isTraceEnabled())
			LOG.trace("update row " + row);

		try {
			//FIXME: Check if the record exists. Not using htable.checkAndPut because it's one column at a time
			//May need to read more HTableInteface's checkAndPut
			Get get = createGet(transID, loc.getBytes());

			//FIXME: need to get the isolation level
			if (transID != null) {
				get.setAttribute(TxnConstants.TRANSACTION_ISOLATION_LEVEL,
		    			Bytes.toBytes(TxnConstants.TransactionIsolationLevel.READ_UNCOMMITED.toString()));
			}
			Result result = htable.get(get);
			if (result.isEmpty()) {
				LOG.error("Row with the key "+ loc.getBytes() +" does not exists. Cannot perform update operation");
				return false;
			}

			Put put = Puts.buildInsert(loc.getBytes(),row,validColumns,transID);
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
        if (useSi) {
            desc.addFamily(TransactionTableCreator.createTransactionFamily());
        }
        return desc;
	}

	public static String getTransIDString(Transaction trans) {
		if (trans == null)
			return null;

		//for debugging purpose right now
		if (!(trans instanceof SpliceTransaction))
			LOG.error("We should only support SpliceTransaction!");

		SpliceTransaction zt = (SpliceTransaction)trans;
		if (zt.getTransactionState() != null && zt.getTransactionState().getTransactionID() != null)
			return zt.getTransactionState().getTransactionID();

		return null;
	}

	public static String getTransID(Transaction trans) {
		String transID = getTransIDString(trans);
		if (transID == null)
			return null;

		return transID;
	}

    private static ITransactionGetsPuts getTransactionGetsPuts() {
        if (useSi) {
            return new SiGetsPuts(TransactorFactory.getDefaultClientTransactor());
        } else {
            return new ZkTransactionGetsPuts();
        }
    }

	public static byte[] getUniqueKey(){
		return gen.next().toString().getBytes();
	}

    public static String getUniqueKeyString() {
        return gen.next().toString();
    }

	public static byte[] generateInstructions(Activation activation,SpliceOperation topOperation) {
        SpliceObserverInstructions instructions = SpliceObserverInstructions.create(activation,topOperation);
        return generateInstructions(instructions);
    }

    public static byte[] generateInstructions(SpliceObserverInstructions instructions) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(out);
            oos.writeObject(instructions);
            oos.flush();
            oos.close();
            return out.toByteArray();
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error generating Splice instructions:" + e.getMessage(), e);
            return null;
        }
    }

    public static void setInstructions(Scan scan, Activation activation, SpliceOperation topOperation){
		scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(activation,topOperation));
	}

    public static void setInstructions(Scan scan, SpliceObserverInstructions instructions){
        //instructions are already set
//        if(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS)!=null) return;
        scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(instructions));
    }

    public static void setThreadContext(LanguageConnectionContext lcc){
        SpliceLogUtils.trace(LOG,"addThreadContext");
        ContextService contextService = ContextService.getFactory();
        ContextManager mgr = contextService.newContextManager();
        mgr.pushContext(lcc);
        contextService.setCurrentContextManager(mgr);
    }

}
