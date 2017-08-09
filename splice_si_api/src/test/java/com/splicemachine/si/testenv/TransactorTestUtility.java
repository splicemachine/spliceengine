/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.testenv;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.WriteConflict;
import com.splicemachine.storage.*;
import com.splicemachine.utils.IntArrays;
import org.junit.Assert;
import java.io.IOException;
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
        try {
            return readAgeDirect(transactorSetup, testEnv, txn, name);
        } catch (Exception e) {
            throw new IOException(e);
        }
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
        RecordScan s = transactorSetup.txnOperationFactory.newDataScan();
        s = s.startKey(key).stopKey(key);

        try (Partition p = testEnv.getPersonTable(transactorSetup)){
            try(RecordScanner results = p.openScanner(s,txn, IsolationLevel.SNAPSHOT_ISOLATION)){
                Record record;
                if((record=results.next())!=null){
                    assertNull(results.next());
                    return readRawTuple(name,record,false,true);
                }else{
                    return "";
                }
            }
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public String scanNoColumns(Txn txn, String name, boolean deleted) throws IOException {
        try {
            return scanNoColumnsDirect(transactorSetup, testEnv, txn, name, deleted);
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    public String scanAll(Txn txn, String startKey, String stopKey, Integer filterValue) throws IOException {

        byte[] key = newRowKey(startKey);
        byte[] endKey = newRowKey(stopKey);
        RecordScan scan = transactorSetup.txnOperationFactory.newDataScan();
        scan.startKey(key).stopKey(endKey);
      //  addPredicateFilter(scan);

        // JL-TODO is filtering part of the transactor?
/*        if (!useSimple && filterValue != null) {
            DataFilter df = transactorSetup.equalsValueFilter(transactorSetup.ageQualifier,convertToBytes(filterValue,filterValue.getClass()));
            scan.filter(df);
*/
//            SingleColumnValueFilter filter = new SingleColumnValueFilter(transactorSetup.family,
//                    transactorSetup.ageQualifier,
//                    CompareFilter.CompareOp.EQUAL,
//                    new BinaryComparator(dataLib.encode(filterValue)));
//            filter.setFilterIfMissing(true);
//            scan.setFilter(filter);
 //       }

        try(Partition p = testEnv.getPersonTable(transactorSetup)){
            try(RecordScanner drs = p.openScanner(scan,txn,IsolationLevel.SNAPSHOT_ISOLATION)){
                StringBuilder result=new StringBuilder();
                Record record;
                while((record = drs.next())!=null){
                    final String name=Bytes.toString((byte[])record.getKey());
                    final String s=readRawTuple(name,record,false,
                            true);
                    result.append(s);
                    if(s.length()>0){
                        result.append("\n");
                    }
                }
                return result.toString();
            }
        }
        catch (Exception e) {
            throw new IOException(e);
        }
    }

    public void assertWriteConflict(IOException e){
        e = testEnv.getExceptionFactory().processRemoteException(e);
        Assert.assertTrue("Expected a WriteConflict exception, but got <"+e.getClass()+">",e instanceof WriteConflict);
    }


    private static void insertField(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                    Txn txn,String name,int index,Object fieldValue) throws IOException {
        Record record = makeRecord(transactorSetup, txn, name, index, fieldValue);
        System.out.println("insert field record -> " + record);
        processRecordDirect(transactorSetup,testEnv, record);
    }

    private static Record makeRecord(TestTransactionSetup transactorSetup,
                                   Txn txn,String name,int index,
                                   Object fieldValue) throws IOException {
        System.out.println("Making Record");
        byte[] key = newRowKey(name);
        System.out.println("key");
        Record record = null;
        try {
            ValueRow row = new ValueRow(1);
            if (index == 0 && fieldValue != null) {
                row.setRowArray(new DataValueDescriptor[]{new SQLInteger((Integer) fieldValue)});
                record = transactorSetup.txnOperationFactory.newRecord(txn,key,new int[]{0}, row);
            } else if (fieldValue != null) {
                row.setRowArray(new DataValueDescriptor[]{new SQLVarchar((String) fieldValue)});
                record = transactorSetup.txnOperationFactory.newRecord(txn,key,new int[]{1}, row);
            }
        } catch (StandardException se) {
            throw new RuntimeException();
        }
        System.out.println("Record Made" + record);
        return record;
    }

    private static void insertFieldBatch(TestTransactionSetup transactorSetup,SITestEnv SITestEnv,
                                         Object[] args,int index) throws IOException {
        Record[] records = new Record[args.length];
        int i = 0;
        for (Object subArgs : args) {
            Object[] subArgsArray = (Object[]) subArgs;
            Txn transactionId = (Txn) subArgsArray[0];
            String name = (String) subArgsArray[1];
            Object fieldValue = subArgsArray[2];
            Record record = makeRecord(transactorSetup, transactionId, name, index, fieldValue);
            records[i] = record;
            i++;
        }
        processPutDirectBatch(transactorSetup,SITestEnv, records);
    }

    private static void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {

        byte[] key = newRowKey(name);
        Record delete = transactorSetup.txnOperationFactory.newDelete(txn, key);
        processMutationDirect(transactorSetup,testEnv,delete);
    }

    private static void processMutationDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                         Record record) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            table.mutate(record,null);
        }
    }

    private static void processRecordDirect(TestTransactionSetup transactorSetup, SITestEnv testEnv,
                                            Record record) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            System.out.println("Perform insert?" + table);
            table.insert(record,null);
        }
    }

    private static void processPutDirectBatch(TestTransactionSetup transactorSetup,
                                              SITestEnv testEnv,
                                              Record[] records) throws IOException {
        try(Partition table = transactorSetup.getPersonTable(testEnv)){
            Iterator<MutationStatus> statusIter=table.writeBatch(null); // JL TODO FIX
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
                                        Txn txn,String name) throws IOException, StandardException {

        byte[] key = newRowKey(name);
        try (Partition p = transactorSetup.getPersonTable(testEnv)){
            Record record =p.get(key,txn,IsolationLevel.SNAPSHOT_ISOLATION);
            return readRawTuple(name,record, true, false);
        }
    }

    private static String scanNoColumnsDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                              Txn txn,String name,boolean deleted) throws StandardException, IOException {
        byte[] endKey = newRowKey(name);
        RecordScan s = transactorSetup.txnOperationFactory.newDataScan();
        s = s.startKey(endKey).stopKey(endKey);
        try(Partition p = transactorSetup.getPersonTable(testEnv)){
            try(RecordScanner recordScanner = p.openScanner(s,txn,IsolationLevel.SNAPSHOT_ISOLATION)){
                Record record = null;
                assertTrue(deleted ||(record=recordScanner.next())!=null);
                assertNull(recordScanner.next());
                return readRawTuple(name,record, false, false);
            }
        }
    }

    private static String readRawTuple(String name,Record record,
                                       boolean singleRowRead,
                                       boolean dumpKeyValues) throws StandardException {
        if (record != null) {
            return resultToStringDirect(name,record);
        }
        if (singleRowRead) {
            return name + " absent";
        } else {
            return "";
        }
    }

    private static String resultToStringDirect(String name,Record result) throws StandardException {
        ValueRow rowDefinition = new ValueRow(2);
        rowDefinition.setRowArray(new DataValueDescriptor[]{new SQLInteger(),new SQLVarchar()});
        result.getData(IntArrays.count(2),rowDefinition);
        return name + " age=" + rowDefinition.getColumn(0).toString() + " job=" + rowDefinition.getColumn(1).toString();
    }
/*
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
*/
    private static String timestampToStableString(Map<Long, String> timestampDecoder, long timestamp) {
        if (timestampDecoder.containsKey(timestamp)) {
            return timestampDecoder.get(timestamp);
        } else {
            final String timestampString = "~" + (9 - timestampDecoder.size());
            timestampDecoder.put(timestamp, timestampString);
            return timestampString;
        }
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
