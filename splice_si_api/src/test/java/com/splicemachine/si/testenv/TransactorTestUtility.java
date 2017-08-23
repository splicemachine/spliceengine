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

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * @author Scott Fines
 *         Date: 2/17/14
 */
public abstract class TransactorTestUtility {

    protected boolean useSimple = true;
    protected SITestEnv testEnv;
    protected TestTransactionSetup transactorSetup;

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

    private void insertAgeDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name,Integer age) throws IOException {
        insertField(transactorSetup,testEnv, txn, name, transactorSetup.agePosition, age);
    }

    private void insertAgeDirectBatch(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                             Object[] args) throws IOException {
        insertFieldBatch(transactorSetup,SITestEnv, args, transactorSetup.agePosition);
    }

    private void insertJobDirect(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                        Txn txn,String name,String job) throws IOException {
        insertField(transactorSetup,SITestEnv, txn, name, transactorSetup.jobPosition, job);
    }


    public abstract String scan(Txn txn, String name) throws IOException;

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


    private void insertField(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                    Txn txn,String name,int index,Object fieldValue) throws IOException {
        DataPut put = makePut(transactorSetup, txn, name, index, fieldValue);
        processPutDirect(transactorSetup,testEnv, put);
    }

    public abstract DataPut makePut(TestTransactionSetup transactorSetup,
                                   Txn txn,String name,int index,
                                   Object fieldValue) throws IOException;

    public void insertFieldBatch(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
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

    public abstract void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException;

    public void processMutationDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
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

    private String readAgeDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {

        byte[] key = newRowKey(name);
        DataGet get = transactorSetup.txnOperationFactory.newDataGet(txn, key,null);
        addPredicateFilter(get);
        try (Partition p = transactorSetup.getPersonTable(testEnv)){
            DataResult rawTuple =p.get(get,null);
            return readRawTuple(name,rawTuple, true, false);
        }
    }

    public String scanNoColumnsDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
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

    public String readRawTuple(String name,DataResult rawTuple,
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

    public abstract String resultToStringDirect(String name,DataResult result);

    public abstract String resultToKeyValueString(DataResult result);

    public String timestampToStableString(Map<Long, String> timestampDecoder, long timestamp) {
        if (timestampDecoder.containsKey(timestamp)) {
            return timestampDecoder.get(timestamp);
        } else {
            final String timestampString = "~" + (9 - timestampDecoder.size());
            timestampDecoder.put(timestamp, timestampString);
            return timestampString;
        }
    }

    public void addPredicateFilter(Attributable operation) throws IOException {
        final BitSet bitSet = new BitSet(2);
        bitSet.set(0);
        bitSet.set(1);
        EntryPredicateFilter filter = new EntryPredicateFilter(bitSet, true);
        operation.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL, filter.toBytes());
    }

    public byte[] newRowKey(Object... args){
        List<byte[]> bytes=new ArrayList<>();
        for(Object a : args){
            bytes.add(convertToBytes(a,a.getClass()));
        }
        return Bytes.concat(bytes);
    }

    public static byte[] convertToBytes(Object value,Class clazz){
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
