/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.testenv;

import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.constants.SIConstants;
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

    public TransactorTestUtility(boolean useSimple,
                                 SITestEnv testEnv,
                                 TestTransactionSetup transactorSetup) {
        this.useSimple = useSimple;
        this.testEnv=testEnv;
        this.transactorSetup = transactorSetup;
    }

    public void insertAge(Txn txn, String name, Integer age) throws IOException {
        insertAgeDirect(transactorSetup,testEnv, txn, name, age);
    }

    public void insertAgeBatch(Object[]... args) throws IOException {
        insertAgeDirectBatch(transactorSetup,testEnv, args);
    }

    public void insertJob(Txn txn, String name, String job) throws IOException {
        insertJobDirect(transactorSetup,testEnv, txn, name, job);
    }

    public void deleteRow(Txn txn, String name) throws IOException {
        deleteRowDirect(transactorSetup,testEnv, txn, name);
    }

    public String read(Txn txn, String name) throws IOException {
        return readAgeDirect(transactorSetup,testEnv, txn, name);
    }

    private static void insertAgeDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name,Integer age) throws IOException {
        insertField(transactorSetup,testEnv, txn, name, transactorSetup.agePosition, age);
    }

    private static void insertAgeDirectBatch(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                             Object[] args) throws IOException {
        insertFieldBatch(transactorSetup,SITestEnv, args, transactorSetup.agePosition);
    }

    private static void insertJobDirect(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                        Txn txn,String name,String job) throws IOException {
        insertField(transactorSetup,SITestEnv, txn, name, transactorSetup.jobPosition, job);
    }


    public String scan(Txn txn, String name) throws IOException {
        byte[] key = newRowKey(name);
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

        byte[] key = newRowKey(startKey);
        byte[] endKey = newRowKey(stopKey);
        DataScan scan = transactorSetup.txnOperationFactory.newDataScan(txn);
        scan.startKey(key).stopKey(endKey);
        addPredicateFilter(scan);

        if (!useSimple && filterValue != null) {
            DataFilter df = transactorSetup.equalsValueFilter(transactorSetup.ageQualifier,convertToBytes(filterValue,filterValue.getClass()));
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
        e = testEnv.getExceptionFactory().processRemoteException(e);
        Assert.assertTrue("Expected a WriteConflict exception, but got <"+e.getClass()+">",e instanceof WriteConflict);
    }


    private static void insertField(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                    Txn txn,String name,int index,Object fieldValue) throws IOException {
        DataPut put = makePut(transactorSetup, txn, name, index, fieldValue);
        processPutDirect(transactorSetup,testEnv, put);
    }

    private static DataPut makePut(TestTransactionSetup transactorSetup,
                                   Txn txn,String name,int index,
                                   Object fieldValue) throws IOException {
        byte[] key = newRowKey(name);
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
        DataPut[] puts = new DataPut[args.length];
        int i = 0;
        for (Object subArgs : args) {
            Object[] subArgsArray = (Object[]) subArgs;
            Txn transactionId = (Txn) subArgsArray[0];
            String name = (String) subArgsArray[1];
            Object fieldValue = subArgsArray[2];
            DataPut put = makePut(transactorSetup, transactionId, name, index, fieldValue);
            puts[i] = put;
            i++;
        }
        processPutDirectBatch(transactorSetup,SITestEnv, puts);
    }

    private static void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {

        byte[] key = newRowKey(name);
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

    private static String readAgeDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {

        byte[] key = newRowKey(name);
        DataGet get = transactorSetup.txnOperationFactory.newDataGet(txn, key,null);
        addPredicateFilter(get);
        try (Partition p = transactorSetup.getPersonTable(testEnv)){
            DataResult rawTuple =p.get(get,null);
            return readRawTuple(name,rawTuple, true, false);
        }
    }

    private static String scanNoColumnsDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                              Txn txn,String name,boolean deleted) throws IOException {
        byte[] endKey = newRowKey(name);
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
//        assertTrue(e.getMessage().startsWith("Failed 1 action: com.splicemachine.si.client.WriteConflict:"));
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

    private static void addPredicateFilter(Attributable operation) throws IOException {
        final BitSet bitSet = new BitSet(2);
        bitSet.set(0);
        bitSet.set(1);
        EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, true);
        operation.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());
    }

    public static byte[] newRowKey(Object... args){
        List<byte[]> bytes=new ArrayList<>();
        for(Object a : args){
            bytes.add(convertToBytes(a,a.getClass()));
        }
        return Bytes.concat(bytes);
    }

    static byte[] convertToBytes(Object value,Class clazz){
        if(clazz==String.class){
            return Bytes.toBytes((String)value);
        }else if(clazz==Integer.class){
            return Bytes.toBytes((Integer)value);
        }else if(clazz==Short.class){
            return Bytes.toBytes((Short)value);
        }else if(clazz==Long.class){
            return Bytes.toBytes((Long)value);
        }else if(clazz==Boolean.class){
            return Bytes.toBytes((Boolean)value);
        }else if(clazz==byte[].class){
            return (byte[])value;
        }else if(clazz==Byte.class){
            return new byte[]{(Byte)value};
        }
        throw new RuntimeException("Unsupported class "+clazz.getName()+" for "+value);
    }
}
