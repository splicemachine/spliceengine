package com.splicemachine.derby.utils;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.Serializer;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.datanucleus.store.valuegenerator.UUIDHexGenerator;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;

/**
 * Utility methods
 * @author jessiezhang
 * @author johnleach
 * @author scottfines
 */

public class SpliceUtils extends SpliceUtilities {
	private static Logger LOG = Logger.getLogger(SpliceUtils.class);
	public static UUIDHexGenerator gen = new UUIDHexGenerator("Splice", null);

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
                        dvd.setValue(1d); //set to one to prevent /-by-zero errors
                        break;
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                        dvd.setValue(1);
                        break;
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        dvd.setValue(false);
                        break;
                    case StoredFormatIds.SQL_LONGINT_ID:
                        dvd.setValue(1l);
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.setValue(1f);
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }

    public static void populateDefaultValues(DataValueDescriptor[] dvds,int defaultValue) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd.isNull()){
                switch(dvd.getTypeFormatId()){
                    case StoredFormatIds.SQL_DOUBLE_ID:
                        dvd.setValue((double)defaultValue); //set to one to prevent /-by-zero errors
                        break;
                    case StoredFormatIds.SQL_SMALLINT_ID:
                    case StoredFormatIds.SQL_INTEGER_ID:
                        dvd.setValue(defaultValue);
                        break;
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        dvd.setValue(false);
                        break;
                    case StoredFormatIds.SQL_LONGINT_ID:
                        dvd.setValue(defaultValue);
                    case StoredFormatIds.SQL_REAL_ID:
                        dvd.setValue(defaultValue);
                    default:
                        //no op, this doesn't have a useful default value
                }
            }
        }
    }

    public static Scan createScan(String transactionId) {
        try {
            return attachTransaction(new Scan(), transactionId);
        } catch (Exception e) {
        	SpliceLogUtils.logAndThrowRuntime(LOG, e);
        	return null;
        }
    }

	public static String getTransactionPath() {
		return zkSpliceTransactionPath;
	}

    public static Get createGet(Mutation mutation, byte[] row) throws IOException {
        return createGet(getTransactionId(mutation), row);
    }

    public static Get createGet(String transactionId, byte[] row) throws IOException {
        return attachTransaction(new Get(row), transactionId);
    }

    public static Get createGet(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, String transID) throws StandardException {
		SpliceLogUtils.trace(LOG,"createGet %s",loc.getBytes());
		try {
			Get get = createGet(transID, loc.getBytes());
			if(validColumns!=null){
				for(int i= validColumns.anySetBit();i!=-1;i = validColumns.anySetBit(i)){
					get.addColumn(DEFAULT_FAMILY_BYTES,Bytes.toBytes(i));
				}
			}else{
				for(int i=0;i<destRow.length;i++){
					get.addColumn(DEFAULT_FAMILY_BYTES,Bytes.toBytes(i));
				}
			}

			return get;
		} catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"createGet Failed",Exceptions.parseException(e));
            return null; //can't happen
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
        return createPut(newRowKey, getTransactionId(mutation));
    }

    public static Put createPut(byte[] newRowKey, String transactionID) throws IOException {
        return attachTransaction(new Put(newRowKey), transactionID);
    }

    /**
     * Attach transactional information to the specified operation.
     *
     * @param op the operation to attach to.
     * @param transactionId the transaction id to attach.
     */
    public static Put attachTransaction(Put op, String transactionId) throws IOException {
        if (!attachTransactionNA(op, transactionId)) {
            getTransactor().initializePut(transactionId, op);
        }
        return op;
    }

    public static Get attachTransaction(Get op, String transactionId) throws IOException {
        if (!attachTransactionNA(op, transactionId)) {
            getTransactor().initializeGet(transactionId, op);
        }
        return op;
    }

    public static Scan attachTransaction(Scan op, String transactionId) throws IOException {
        if (!attachTransactionNA(op, transactionId)) {
            getTransactor().initializeScan(transactionId, op);
        }
        return op;
    }

    private static boolean attachTransactionNA(OperationWithAttributes op, String transactionId) {
        if (transactionId == null) {
            throw new RuntimeException("Cannot create operation with a null transactionId");
        }
        if (transactionId.equals(NA_TRANSACTION_ID)) {
            op.setAttribute(SI_EXEMPT, Bytes.toBytes(true));
            return true;
        }
        return false;
    }

    public static Put createDeletePut(Mutation mutation, byte[] rowKey) {
        return createDeletePut(getTransactionId(mutation), rowKey);
    }

    public static Put createDeletePut(String transactionId, byte[] rowKey) {
        final ClientTransactor<Put, Get, Scan, Mutation, byte[]> clientTransactor = HTransactorFactory.getClientTransactor();
        return clientTransactor.createDeletePut(clientTransactor.transactionIdFromString(transactionId), rowKey);
    }

    public static boolean isDelete(Mutation mutation) {
        if(mutation instanceof Delete) {
            return true;
        } else {
            return HTransactorFactory.getClientTransactor().isDeletePut(mutation);
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
        return getTransactor().transactionIdFromPut((Put) mutation).getTransactionIdString();
    }

    public static void handleNullsInUpdate(Put put, DataValueDescriptor[] row, FormatableBitSet validColumns) {
        if (validColumns != null) {
            int numrows = (validColumns != null ? validColumns.getLength() : row.length);  // bug 118
            for (int i = 0; i < numrows; i++) {
                if (validColumns.isSet(i) && row[i] != null && row[i].isNull())
                    put.add(SpliceConstants.DEFAULT_FAMILY.getBytes(), (new Integer(i)).toString().getBytes(), 0, SIConstants.EMPTY_BYTE_ARRAY);
            }
        }
    }

	public static void populate(KeyValue[] raw, DataValueDescriptor[] destRow) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",raw.length,destRow.length);
		/**
		 * We have to use dataMap here instead of using currentResult.getValue() because for some reason columns larger
		 * than 9 will go missing if you call getValue() --likely its due to the fact that we are serializing ints
		 * as strings instead of as ints themselves.
		 */		
		try{
			for(int i=0;i<destRow.length;i++){
				if (i<raw.length)
					fill(raw[i].getValue(),destRow[i]);
				else 
					fill(null,destRow[i]);
			}
		}catch(IOException ioe){
			SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
		}
	}

    public static void populate(Result currentResult, DataValueDescriptor[] destRow,Serializer serializer) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",currentResult.raw().length,destRow.length);
        /**
         * We have to use dataMap here instead of using currentResult.getValue() because for some reason columns larger
         * than 9 will go missing if you call getValue() --likely its due to the fact that we are serializing ints
         * as strings instead of as ints themselves.
         */
        Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
        try{
            for(int i=0;i<destRow.length;i++){
                byte[] value = dataMap.get(Bytes.toBytes(i));
                fill(value,destRow[i],serializer);
            }
        }catch(IOException ioe){
            SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
        }
    }

    public static void populate(Result currentResult, FormatableBitSet scanColumnList, DataValueDescriptor[] destRow,Serializer serializer) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG,"populate current Result %s using scanColumnList %s and destRow with size %d",currentResult,scanColumnList,destRow.length);
        try {
            if(scanColumnList == null) populate(currentResult,destRow,serializer);
            else{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
                for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
                    byte[] value = dataMap.get(Bytes.toBytes(i));
                    fill(value,destRow[i],serializer);
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error occurred during populate", e);
        }
    }

    public static void populate(Result currentResult, FormatableBitSet scanColumnList, DataValueDescriptor[] destRow) throws StandardException {
        if (LOG.isTraceEnabled())
        	SpliceLogUtils.trace(LOG,"populate current Result %s using scanColumnList %s and destRow with size %d",currentResult,scanColumnList,destRow.length);
        try {
            if(scanColumnList == null) 
            	populate(currentResult.raw(),destRow);
            else{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
                for(int i=scanColumnList.anySetBit();i!=-1;i=scanColumnList.anySetBit(i)){
                    byte[] value = dataMap.get(Bytes.toBytes(i));
                    fill(value,destRow[i]);
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error occurred during populate", e);
        }
    }



    public static void populate(List<KeyValue> keyValues, DataValueDescriptor[] destRow,
                                FormatableBitSet scanList,int[] bitSetToDestRowMap,Serializer serializer) throws StandardException{
        if(scanList==null||scanList.getNumBitsSet()<=0) populate(keyValues,destRow,serializer);
        else {
            try{
                int placeHolder = 0;
                for (KeyValue keyValue : keyValues) {
                    if (Bytes.compareTo(keyValue.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) == 0) // Check for SI family in the case of count(*)
                        continue;
                    else if(Bytes.compareTo(keyValue.getQualifier(), SpliceConstants.TASK_ID_COL)==0)
                        continue; //skip the task column
                    placeHolder = Bytes.toInt(keyValue.getQualifier());
                    if (scanList.isSet(placeHolder)) {
                        fill(keyValue.getValue(),destRow[bitSetToDestRowMap[placeHolder]],serializer);
                    }
                }
            }catch(IOException e){
                SpliceLogUtils.logAndThrowRuntime(LOG,"Error occurred during populate",e);
            }
        }
    }

    
    public static void populate(Result currentResult, DataValueDescriptor[] destRow,
                                FormatableBitSet scanList,int[] bitSetToDestRowMap,Serializer serializer) throws StandardException{
        if(scanList==null||scanList.getNumBitsSet()<=0) populate(currentResult,destRow,serializer);
        else{
            try{
                Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
                if (dataMap != null) {
                    for (int i = scanList.anySetBit(); i != -1; i = scanList.anySetBit(i)) {
                        byte[] value = dataMap.get(Bytes.toBytes(i));
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
		if(scanList==null||scanList.getNumBitsSet()<=0) populate(currentResult.raw(),destRow);
		else{
			try{
				Map<byte[],byte[]> dataMap = currentResult.getFamilyMap(SpliceConstants.DEFAULT_FAMILY_BYTES);
				for(int i=scanList.anySetBit();i!=-1;i=scanList.anySetBit(i)){
					byte[] value = dataMap.get(Bytes.toBytes(i));
					fill(value, destRow[bitSetToDestRowMap[i]]);
				}
			}catch(IOException e){
				SpliceLogUtils.logAndThrowRuntime(LOG,"Error occurred during populate",e);
			}
		}
	}
    

	public static void populate(List<KeyValue> keyValues, DataValueDescriptor[] destRow) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",keyValues.size(),destRow.length);
			try {
			int position;
			for (KeyValue keyValue: keyValues) {
				if (Bytes.compareTo(keyValue.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) == 0) // Check for SI family in the case of count(*)
					continue;
                else if(Bytes.compareTo(keyValue.getQualifier(), SpliceUtils.TASK_ID_COL)==0)
                    continue; //skip the task column
				position = Bytes.toInt(keyValue.getQualifier());
				if (destRow.length -1 >= position) {
					fill(keyValue.getValue(),destRow[Bytes.toInt(keyValue.getQualifier())]);
				} else {
					SpliceLogUtils.warn(LOG, "populate - warn position %d %s", position, keyValue);
				}
			}
		}catch(IOException e){
            SpliceLogUtils.logAndThrow(LOG,"populate error", Exceptions.parseException(e));
		}
	}
    
    public static void populate(List<KeyValue> keyValues, DataValueDescriptor[] destRow,FormatableBitSet scanList,int[] bitSetToDestRowMap) throws StandardException{
    	if(scanList==null||scanList.getNumBitsSet()<=0) populate(keyValues,destRow);
    	else {
    		try{
    			int placeHolder = 0;
    			for (KeyValue keyValue : keyValues) {
    				if (Bytes.compareTo(keyValue.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) == 0) // Check for SI family in the case of count(*)
    					continue;
                    else if(Bytes.compareTo(keyValue.getQualifier(), SpliceUtils.TASK_ID_COL)==0)
                        continue; //skip the task column
    				placeHolder = Bytes.toInt(keyValue.getQualifier());
    				if (scanList.isSet(placeHolder)) {
    					fill(keyValue.getValue(),destRow[bitSetToDestRowMap[placeHolder]]);
    				}
    			}
    		}catch(IOException e){
                SpliceLogUtils.logAndThrow(LOG,"populate error",Exceptions.parseException(e));
    		}
    	}
}

    public static void populate(List<KeyValue> keyValues, DataValueDescriptor[] destRow,Serializer serializer) throws StandardException {
        if (LOG.isTraceEnabled())
        	SpliceLogUtils.trace(LOG, "fully populating current Result with size %d into row of size %d",keyValues.size(),destRow.length);
        try{
    		int placeHolder = 0;
			for (KeyValue keyValue : keyValues) {
				if (Bytes.compareTo(keyValue.getFamily(),SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) == 0) // Check for SI family in the case of count(*)
					continue;
				placeHolder = Bytes.toInt(keyValue.getQualifier());
				fill(keyValue.getValue(),destRow[placeHolder],serializer);
			}
	     }catch(IOException ioe) {
            SpliceLogUtils.logAndThrow(LOG, StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,ioe));
        }
    }

    

	public static SpliceObserverInstructions getSpliceObserverInstructions(Scan scan) {
		byte[] instructions = scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS);
		if(instructions==null) return null;
		//Putting this here to prevent some kind of weird NullPointer situation
		//where the LanguageConnectionContext doesn't get initialized properly
		ByteArrayInputStream bis = null;
		ObjectInputStream ois = null;
		try {
			bis = new ByteArrayInputStream(instructions);
			ois = new ObjectInputStream(bis);
			SpliceObserverInstructions soi = (SpliceObserverInstructions) ois.readObject();
			return soi;
		} catch (Exception e) {
			Closeables.closeQuietly(ois);
			Closeables.closeQuietly(bis);			
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
	
	public static void setQueryWaitNode(String uniqueSequenceID, Watcher watcher) {
		SpliceLogUtils.debug(LOG, "setQueryWaitNode");
		try {
			ZkUtils.getData(uniqueSequenceID, watcher, null);
		} catch (IOException e) {
			e.printStackTrace(); // TODO JL - FIX
		}
	}

	public static String getTransIDString(Transaction trans) {
		if (trans == null)
			return null;

		//for debugging purpose right now
		if (!(trans instanceof SpliceTransaction))
			LOG.error("We should only support SpliceTransaction!");

		SpliceTransaction spliceTransaction = (SpliceTransaction)trans;
		if (spliceTransaction.getTransactionId() != null && spliceTransaction.getTransactionId().getTransactionIdString() != null)
			return spliceTransaction.getTransactionId().getTransactionIdString();

		return null;
	}


	public static String getTransID(Transaction trans) {
		String transID = getTransIDString(trans);
		if (transID == null)
			return null;

		return transID;
	}

    protected static ClientTransactor<Put, Get, Scan, Mutation, byte[]> getTransactor() {
        return HTransactorFactory.getClientTransactor();
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
        scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(instructions));
    }

    public static void setThreadContext(LanguageConnectionContext lcc){
        SpliceLogUtils.trace(LOG,"addThreadContext");
        ContextService contextService = ContextService.getFactory();
        ContextManager mgr = contextService.newContextManager();
        mgr.pushContext(lcc);
        contextService.setCurrentContextManager(mgr);
    }
    public static boolean propertyExists(String propertyName) throws StandardException {
    	SpliceLogUtils.trace(LOG, "propertyExists %s",propertyName);
    	RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
        try {
        	return rzk.exists(zkSpliceDerbyPropertyPath + "/" + propertyName, false) != null;
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"propertyExistsException",Exceptions.parseException(e));
            return false; //can't happen
        }
    }

    public static byte[] getProperty(String propertyName) throws StandardException {
    	SpliceLogUtils.trace(LOG, "propertyExists %s",propertyName);
    	RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
        try {
        	return rzk.getData(zkSpliceDerbyPropertyPath + "/" + propertyName, false, null);
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG,"propertyExists Exception",Exceptions.parseException(e));
            return null; //can't happen
        }
    }

    public static void addProperty(String propertyName, String propertyValue) throws StandardException {
    		SpliceLogUtils.trace(LOG, "addProperty name %s , value %s", propertyName, propertyValue);
    	   	RecoverableZooKeeper rzk = ZkUtils.getRecoverableZooKeeper();
            try {
                    rzk.create(zkSpliceDerbyPropertyPath + "/" + propertyName, Bytes.toBytes(propertyValue), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            } catch (Exception e) {
                SpliceLogUtils.logAndThrow(LOG,"addProperty Exception",Exceptions.parseException(e));
            }
    }
}
