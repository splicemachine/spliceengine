package com.splicemachine.si.testenv;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import java.io.IOException;
import com.carrotsearch.hppc.BitSet;

import java.util.*;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Scott Fines
 *         Date: 2/17/14
 */
public class TransactorTestUtility {

    private boolean useSimple = true;
    private SITestEnv testEnv;
    private TestTransactionSetup transactorSetup;
    private SDataLib dataLib;

    public TransactorTestUtility(boolean useSimple,
                                 SITestEnv testEnv,
                                 TestTransactionSetup transactorSetup) {
        this.useSimple = useSimple;
        this.testEnv=testEnv;
        this.transactorSetup = transactorSetup;
        this.dataLib = testEnv.getDataLib();
    }

    public void insertAge(Txn txn, String name, Integer age) throws IOException {
        insertAgeDirect(useSimple, transactorSetup,testEnv, txn, name, age);
    }

    public void insertAgeBatch(Object[]... args) throws IOException {
        insertAgeDirectBatch(useSimple, transactorSetup,testEnv, args);
    }

    public void insertJob(Txn txn, String name, String job) throws IOException {
        insertJobDirect(useSimple, transactorSetup,testEnv, txn, name, job);
    }

    public void deleteRow(Txn txn, String name) throws IOException {
        deleteRowDirect(transactorSetup,testEnv, txn, name);
    }

    public String read(Txn txn, String name) throws IOException {
        return readAgeDirect(transactorSetup,testEnv, txn, name);
    }

    public static void insertAgeDirect(boolean useSimple, TestTransactionSetup transactorSetup, SITestEnv testEnv,
                                       Txn txn, String name, Integer age) throws IOException {
        insertField(transactorSetup,testEnv, txn, name, transactorSetup.agePosition, age);
    }

    public static void insertAgeDirectBatch(boolean useSimple, TestTransactionSetup transactorSetup, SITestEnv SITestEnv,
                                            Object[] args) throws IOException {
        insertFieldBatch(transactorSetup,SITestEnv, args, transactorSetup.agePosition);
    }

    public static void insertJobDirect(boolean useSimple, TestTransactionSetup transactorSetup, SITestEnv SITestEnv,
                                       Txn txn, String name, String job) throws IOException {
        insertField(transactorSetup,SITestEnv, txn, name, transactorSetup.jobPosition, job);
    }


    public String scan(Txn txn, String name) throws IOException {
        byte[] key = dataLib.newRowKey(new Object[]{name});
        DataScan s = transactorSetup.txnOperationFactory.newDataScan(txn);
        s = s.startKey(key).stopKey(key);

        try (Partition p = testEnv.getPersonTable(transactorSetup)){
            try(DataResultScanner results = p.openResultScanner(s)){
                DataResult dr;
                if((dr=results.next())!=null){
                    assertNull(results.next());
                    return readRawTuple(name,dr,false,true);
                }else{
                    return "";
                }
            }
        }
    }

    public String scanNoColumns(Txn txn, String name, boolean deleted) throws IOException {
        return scanNoColumnsDirect(transactorSetup,testEnv, txn, name, deleted);
    }

    public String scanAll(Txn txn, String startKey, String stopKey, Integer filterValue) throws IOException {

        byte[] key = dataLib.newRowKey(new Object[]{startKey});
        byte[] endKey = dataLib.newRowKey(new Object[]{stopKey});
        DataScan scan = transactorSetup.txnOperationFactory.newDataScan(txn);
        scan.startKey(key).stopKey(endKey);
        addPredicateFilter(scan);

        if (!useSimple && filterValue != null) {
            DataFilter df = transactorSetup.equalsValueFilter(transactorSetup.ageQualifier,dataLib.encode(filterValue));
            scan.filter(df);
//            SingleColumnValueFilter filter = new SingleColumnValueFilter(transactorSetup.family,
//                    transactorSetup.ageQualifier,
//                    CompareFilter.CompareOp.EQUAL,
//                    new BinaryComparator(dataLib.encode(filterValue)));
//            filter.setFilterIfMissing(true);
//            scan.setFilter(filter);
        }
        try(Partition p = testEnv.getPersonTable(transactorSetup)){
            try(DataResultScanner drs = p.openResultScanner(scan)){
                StringBuilder result=new StringBuilder();
                DataResult dr;
                while((dr = drs.next())!=null){
                    final String name=Bytes.toString(dr.key());
                    final String s=readRawTuple(name,dr,false,
                            true);
                    result.append(s);
                    if(s.length()>0){
                        result.append("\n");
                    }
                }
                return result.toString();
            }
        }
    }

    public void assertWriteConflict(IOException e){
       Assert.assertTrue("Expected a WriteConflict exception, but got <"+e.getClass()+">",e instanceof WriteConflict);
    }


    private static void insertField(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                    Txn txn,String name,int index,Object fieldValue) throws IOException {
        final SDataLib dataLib = testEnv.getDataLib();
        DataPut put = makePut(transactorSetup, txn, name, index, fieldValue, dataLib);
        processPutDirect(transactorSetup,testEnv, put);
    }

    private static DataPut makePut(TestTransactionSetup transactorSetup,
                                                   Txn txn, String name, int index,
                                                   Object fieldValue, SDataLib dataLib) throws IOException {
        byte[] key = dataLib.newRowKey(new Object[]{name});
        DataPut put = transactorSetup.txnOperationFactory.newDataPut(txn,key);
        final BitSet bitSet = new BitSet();
        if (fieldValue != null)
            bitSet.set(index);
        final EntryEncoder entryEncoder = EntryEncoder.create(new KryoPool(1),2,bitSet,null,null,null);
        try {
            if (index == 0 && fieldValue != null) {
                entryEncoder.getEntryEncoder().encodeNext((Integer) fieldValue);
            } else if (fieldValue != null) {
                entryEncoder.getEntryEncoder().encodeNext((String) fieldValue);
            }
            put.addCell(transactorSetup.family,SIConstants.PACKED_COLUMN_BYTES,txn.getTxnId(),entryEncoder.encode());
        } finally {
            entryEncoder.close();
        }
        return put;
    }

    private static void insertFieldBatch(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                         Object[] args,int index) throws IOException {
        final SDataLib dataLib = SITestEnv.getDataLib();
        DataPut[] puts = new DataPut[args.length];
        int i = 0;
        for (Object subArgs : args) {
            Object[] subArgsArray = (Object[]) subArgs;
            Txn transactionId = (Txn) subArgsArray[0];
            String name = (String) subArgsArray[1];
            Object fieldValue = subArgsArray[2];
            DataPut put = makePut(transactorSetup, transactionId, name, index, fieldValue, dataLib);
            puts[i] = put;
            i++;
        }
        processPutDirectBatch(transactorSetup,SITestEnv, puts);
    }

    static void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                Txn txn,String name) throws IOException {
        final SDataLib dataLib = testEnv.getDataLib();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        DataMutation put = transactorSetup.txnOperationFactory.newDataDelete(txn, key);
        processMutationDirect(transactorSetup,testEnv,put);
//        if(put instanceof DataPut)
//            processPutDirect(transactorSetup,testEnv,(DataPut)put);
//        else
//            processDeleteDirect(transactorSetup,testEnv,(DataDelete)put);
    }

    private static void processMutationDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                         DataMutation put) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            table.mutate(put);
        }
    }

    private static void processPutDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                         DataPut put) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            table.put(put);
        }
    }

    private static void processDeleteDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                            DataDelete put) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            table.delete(put);
        }
    }

    private static void processPutDirectBatch(TestTransactionSetup transactorSetup,
                                              SITestEnv testEnv,
                                              DataPut[] puts) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            Iterator<MutationStatus> statusIter=table.writeBatch(puts);
            int i=0;
            while(statusIter.hasNext()){
                MutationStatus next=statusIter.next();
                Assert.assertTrue("Row "+i+" did not return success!",next.isSuccess());
                Assert.assertFalse("Row "+i+" returned failure!",next.isFailed());
                Assert.assertFalse("Row "+i+" returned not run!",next.isNotRun());
                i++;
            }
        }
    }

    static String readAgeDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                Txn txn,String name) throws IOException {
        final SDataLib dataLib = testEnv.getDataLib();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        DataGet get = transactorSetup.txnOperationFactory.newDataGet(txn, key,null);
        addPredicateFilter(get);
        try (Partition p = transactorSetup.getPersonTable(testEnv)){
            DataResult rawTuple =p.get(get,null);
            return readRawTuple(name,rawTuple, true, false);
        }
    }

    static String scanNoColumnsDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                      Txn txn,String name,boolean deleted) throws IOException {
        final SDataLib dataLib = testEnv.getDataLib();

        byte[] endKey = dataLib.newRowKey(new Object[]{name});
        DataScan s = transactorSetup.txnOperationFactory.newDataScan(txn);
        s = s.startKey(endKey).stopKey(endKey);
        addPredicateFilter(s);
        transactorSetup.readController.preProcessScan(s);
        try(Partition p = transactorSetup.getPersonTable(testEnv)){
            try(DataResultScanner drs = p.openResultScanner(s)){
                DataResult dr = null;
                assertTrue(deleted ||(dr=drs.next())!=null);
                assertNull(drs.next());
                return readRawTuple(name,dr, false, false);
            }
        }
    }

    private static String readRawTuple(String name,DataResult rawTuple,
                                       boolean singleRowRead,
                                       boolean dumpKeyValues) throws IOException {
        if (rawTuple != null && rawTuple.size()>0) {
            String suffix = dumpKeyValues ? "[ " + resultToKeyValueString(rawTuple) + " ]" : "";
            return resultToStringDirect(name,rawTuple) + suffix;
        }
        if (singleRowRead) {
            return name + " absent";
        } else {
            return "";
        }
    }

    public String resultToString(String name, DataResult result) {
        return resultToStringDirect(name,result);
    }

    private static String resultToStringDirect(String name,DataResult result) {
        byte[] packedColumns = result.userData().value();
        EntryDecoder decoder = new EntryDecoder();
        decoder.set(packedColumns);
        MultiFieldDecoder mfd = null;
        try {
            mfd = decoder.getEntryDecoder();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        Integer age = null;
        String job = null;
        if (decoder.isSet(0))
            age = mfd.decodeNextInt();
        if (decoder.isSet(1))
            job = mfd.decodeNextString();
        return name + " age=" + age + " job=" + job;
    }

    private static String resultToKeyValueString(DataResult result) {
        Map<Long, String> timestampDecoder =new HashMap<>();
        final StringBuilder s = new StringBuilder();
        byte[] packedColumns = result.userData().value();
        long timestamp = result.userData().version();
        EntryDecoder decoder = new EntryDecoder();
        decoder.set(packedColumns);
        MultiFieldDecoder mfd = null;
        try {
            mfd = decoder.getEntryDecoder();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        if (decoder.isSet(0))
            s.append("V.age@" + timestampToStableString(timestampDecoder, timestamp) + "=" + mfd.decodeNextInt());
        if (decoder.isSet(1))
            s.append("V.job@" + timestampToStableString(timestampDecoder, timestamp) + "=" + mfd.decodeNextString());
        return s.toString();
    }

//    public void assertWriteConflict(RetriesExhaustedWithDetailsException e) {
//        Assert.assertEquals(1, e.getNumExceptions());
//        assertTrue(e.getMessage().startsWith("Failed 1 action: com.splicemachine.si.impl.WriteConflict:"));
//    }

    private static String timestampToStableString(Map<Long, String> timestampDecoder, long timestamp) {
        if (timestampDecoder.containsKey(timestamp)) {
            return timestampDecoder.get(timestamp);
        } else {
            final String timestampString = "~" + (9 - timestampDecoder.size());
            timestampDecoder.put(timestamp, timestampString);
            return timestampString;
        }
    }

    public DataResult readRaw(String name) throws IOException {
        return readAgeRawDirect(testEnv, name, true);
    }

    public DataResult readRaw(String name, boolean allVersions) throws IOException {
        return readAgeRawDirect(testEnv, name, allVersions);
    }

    static DataResult readAgeRawDirect(SITestEnv SITestEnv, String name, boolean allversions) throws IOException {
        final SDataLib dataLib = SITestEnv.getDataLib();

        byte[] key = dataLib.newRowKey(new Object[]{name});
        Attributable get = makeGet(dataLib, key);
        if (allversions) {
            dataLib.setGetMaxVersions(get);
        }
        try(Partition table =SIDriver.getTableFactory().getTable(SITestEnv.getPersonTableName())){
            return (DataResult)table.get(get);
        }
    }

    public static Attributable makeGet(SDataLib dataLib, byte[] key) throws IOException {
        final Attributable get = (Attributable)dataLib.newGet(key, null, null, null);
        addPredicateFilter(get);
        return get;
    }

    private Object makeScan(SDataLib dataLib, byte[] endKey, ArrayList families, byte[] startKey) throws IOException {
        final Attributable scan = (Attributable) dataLib.newScan(startKey, endKey, families, null, null);
        addPredicateFilter(scan);
        return scan;
    }

    private static void addPredicateFilter(Attributable operation) throws IOException {
        final BitSet bitSet = new BitSet(2);
        bitSet.set(0);
        bitSet.set(1);
        EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, new ObjectArrayList<Predicate>(), true);
        operation.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());
    }

}
