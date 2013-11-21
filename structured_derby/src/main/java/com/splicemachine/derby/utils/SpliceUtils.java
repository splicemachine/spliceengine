package com.splicemachine.derby.utils;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;
import com.splicemachine.utils.kryo.KryoObjectInput;
import com.splicemachine.utils.kryo.KryoObjectOutput;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.store.raw.Transaction;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import java.io.*;

/**
 * 
 * Splice Utility Methods
 * 
 */

public class SpliceUtils extends SpliceUtilities {
	private static Logger LOG = Logger.getLogger(SpliceUtils.class);

    /**
     * Populates an array of DataValueDescriptors with a default value based on their type.
     *
     * This is used mainly to prevent NullPointerExceptions from occurring in administrative
     * operations such as getExecRowDefinition().
     *
     * @param dvds
     * @throws StandardException
     */
    public static void populateDefaultValues(DataValueDescriptor[] dvds,int defaultValue) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd != null && dvd.isNull()){
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
        return createScan(transactionId, true);
    }

    public static Scan createScan(String transactionId, boolean includeSI) {
        try {
            return attachTransaction(new Scan(), transactionId, includeSI);
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, e);
            return null;
        }
    }

    public static Get createGet(Mutation mutation, byte[] row) throws IOException {
        return createGet(getTransactionId(mutation), row);
    }

    public static Get createGet(String transactionId, byte[] row) throws IOException {
        return attachTransaction(new Get(row), transactionId);
    }

    public static Get createGet(RowLocation loc, DataValueDescriptor[] destRow, FormatableBitSet validColumns, String transID) throws StandardException {
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createGet %s",loc.getBytes());
		try {
			Get get = createGet(transID, loc.getBytes());
            get.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY);
            BitSet fieldsToReturn;
            if(validColumns!=null){
                fieldsToReturn = new BitSet(validColumns.size());
                for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
                    fieldsToReturn.set(i);
                }
            }else{
                fieldsToReturn = new BitSet(destRow.length);
                fieldsToReturn.set(0,destRow.length);
            }
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn, new ObjectArrayList<Predicate>());
            get.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
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

    public static Scan attachTransaction(Scan op, String transactionId, boolean includeSI) throws IOException {
        if (!attachTransactionNA(op, transactionId)) {
            getTransactor().initializeScan(transactionId, op, includeSI);
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
        TransactionId transactionId = getTransactor().transactionIdFromPut((Put) mutation);
        if(transactionId==null) return null;
        return transactionId.getTransactionIdString();
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


	public static SpliceObserverInstructions getSpliceObserverInstructions(Scan scan) {
		byte[] instructions = scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS);
		if(instructions==null) return null;

        Kryo kryo = SpliceDriver.getKryoPool().get();
		try {
            Input input = new Input(instructions);
            KryoObjectInput koi = new KryoObjectInput(input,kryo);
			SpliceObserverInstructions soi = (SpliceObserverInstructions) koi.readObject();
			return soi;
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
		}finally{
            SpliceDriver.getKryoPool().returnInstance(kryo);
        }
		return null;
	}

//	public static boolean update(RowLocation loc, DataValueDescriptor[] row,
//			FormatableBitSet validColumns, HTableInterface htable, String transID) throws StandardException {
//		if (LOG.isTraceEnabled())
//			LOG.trace("update row " + row);
//
//		try {
//			//FIXME: Check if the record exists. Not using htable.checkAndPut because it's one column at a time
//			//May need to read more HTableInteface's checkAndPut
//			Get get = createGet(transID, loc.getBytes());
//
//			Result result = htable.get(get);
//			if (result.isEmpty()) {
//				LOG.error("Row with the key "+ loc.getBytes() +" does not exists. Cannot perform update operation");
//				return false;
//			}
//
//            Put put = createPut(loc.getBytes(),transID);
//            RowMarshaller.sparsePacked().encodeRow(row, bitSetToMap(validColumns), put, null);
//			//FIXME: checkAndPut can only do one column at a time, too expensive
//			htable.put(put);
//			return true;
//		} catch (IOException ie) {
//			LOG.error(ie.getMessage(), ie);
//		}
//		return false;
//	}
	
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
        return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
	}

    public static String getUniqueKeyString() {
        return Long.toString(SpliceDriver.driver().getUUIDGenerator().nextUUID());
    }

	public static byte[] generateInstructions(Activation activation,SpliceOperation topOperation, SpliceRuntimeContext spliceRuntimeContext) {
        SpliceObserverInstructions instructions = SpliceObserverInstructions.create(activation,topOperation,spliceRuntimeContext);
        return generateInstructions(instructions);
    }

    public static byte[] generateInstructions(SpliceObserverInstructions instructions) {
        Kryo kryo = SpliceDriver.getKryoPool().get();
        try {
            Output output = new Output(4096,-1);
            KryoObjectOutput koo = new KryoObjectOutput(output,kryo);
            koo.writeObject(instructions);
            return output.toBytes();
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Error generating Splice instructions:" + e.getMessage(), e);
            return null;
        }finally{
            SpliceDriver.getKryoPool().returnInstance(kryo);
        }
    }

    public static void setInstructions(Scan scan, Activation activation, SpliceOperation topOperation, SpliceRuntimeContext spliceRuntimeContext){
		scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(activation,topOperation,spliceRuntimeContext));
	}

    public static void setInstructions(Scan scan, SpliceObserverInstructions instructions){
        scan.setAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS,generateInstructions(instructions));
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

    public static int[] bitSetToMap(FormatableBitSet bitSet){
        if(bitSet==null) return null;
        int[] validCols = new int[bitSet.getNumBitsSet()];
        int pos=0;
        for(int i=bitSet.anySetBit();i!=-1;i=bitSet.anySetBit(i)){
            validCols[pos] = i;
            pos++;
        }
        return validCols;
    }

    public static int[] translateToZeroIndexed(int[] oneIndexed) {
        if(oneIndexed==null) return null;
        if(oneIndexed.length<=0) return oneIndexed;
        int[] zeroIndexed = new int[oneIndexed.length];
        for(int i=0;i<oneIndexed.length;i++){
            zeroIndexed[i] = oneIndexed[i] -1;
        }
        return zeroIndexed;
    }
}
