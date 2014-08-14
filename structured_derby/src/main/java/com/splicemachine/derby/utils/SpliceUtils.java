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
import com.splicemachine.si.api.TransactionStorage;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.SimpleOperationFactory;
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
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Map;

/**
 *
 * Splice Utility Methods
 *
 */

public class SpliceUtils extends SpliceUtilities {
    private static Logger LOG = Logger.getLogger(SpliceUtils.class);
    private static final String hostName;
    private static final TxnOperationFactory operationFactory;
    static{
        try {
            hostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }
        operationFactory = new SimpleOperationFactory(TransactionStorage.getTxnSupplier());
    }

    /**
     * Populates an array of DataValueDescriptors with a default value based on their type.
     *
     * This is used mainly to prevent NullPointerExceptions from occurring in administrative
     * operations such as getExecRowDefinition().
     *
     * @param dvds the descriptors to populate
     * @param defaultValue the value to default each descriptor to
     *
     * @throws StandardException
     */
    public static void populateDefaultValues(DataValueDescriptor[] dvds,int defaultValue) throws StandardException{
        for(DataValueDescriptor dvd:dvds){
            if(dvd != null){
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

    public static Scan createScan(Txn txn) {
        return createScan(txn,false);
    }

    public static Scan createScan(Txn txn,boolean countStar) {
        return operationFactory.newScan(txn,countStar);
    }

    public static Get createGet(TxnView txn, byte[] row) throws IOException {
        return operationFactory.newGet(txn, row);
    }

    public static Get createGet(RowLocation loc,
                                DataValueDescriptor[] destRow,
                                FormatableBitSet validColumns, Txn txn) throws StandardException {
        try {
            Get get = createGet(txn, loc.getBytes());
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
     * @param txn the transaction to delete under
     * @param row the row to delete.
     * @throws IOException if something goes wrong during deletion.
     */
    public static void doDelete(HTableInterface table, Txn txn, byte[] row) throws IOException {
        Mutation mutation = operationFactory.newDelete(txn, row);
        if(mutation instanceof Put)
            table.put((Put)mutation);
        else
            table.delete((Delete)mutation);
    }


    public static Put createPut(byte[] newRowKey, Txn txn) throws IOException {
        return operationFactory.newPut(txn,newRowKey);
    }

    public static boolean isDelete(Mutation mutation) {
        return mutation instanceof Delete || mutation.getAttribute(SIConstants.SI_DELETE_PUT)!=null;
    }

    public static SpliceObserverInstructions getSpliceObserverInstructions(Scan scan) {
        byte[] instructions = scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS);
        if(instructions==null) return null;

        Kryo kryo = SpliceDriver.getKryoPool().get();
        try {
            Input input = new Input(instructions);
            KryoObjectInput koi = new KryoObjectInput(input,kryo);
            return (SpliceObserverInstructions) koi.readObject();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "Issues reading serialized data",e);
        }finally{
            SpliceDriver.getKryoPool().returnInstance(kryo);
        }
        return null;
    }

    public static byte[] getUniqueKey(){
        return SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
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

    public static BitSet bitSetFromBooleanArray(boolean[] array) {
        BitSet bitSet = new BitSet(array.length);
        for (int col = 0; col < array.length; col++) {
            if (array[col])
                bitSet.set(col);
        }
        return bitSet;
    }

    public static String getHostName() {
        return hostName;
    }

    private static void copyAttributes(OperationWithAttributes source, OperationWithAttributes dest) {
        Map<String,byte[]> attributesMap = source.getAttributesMap();
        for(Map.Entry<String,byte[]> attribute:attributesMap.entrySet()){
            dest.setAttribute(attribute.getKey(),attribute.getValue());
        }
    }
}
