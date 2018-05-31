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
public class SITransactorTestUtility extends TransactorTestUtility {

    public SITransactorTestUtility(boolean useSimple,
                                   SITestEnv testEnv,
                                   TestTransactionSetup transactorSetup) {
        super(useSimple,testEnv,transactorSetup);
    }

    @Override
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

    public DataPut makePut(TestTransactionSetup transactorSetup,
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

    public void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {

        byte[] key = newRowKey(name);
        DataMutation put = transactorSetup.txnOperationFactory.newDataDelete(txn, key);
        processMutationDirect(transactorSetup,testEnv,put);
//        if(put instanceof DataPut)
//            processPutDirect(transactorSetup,testEnv,(DataPut)put);
//        else
//            processDeleteDirect(transactorSetup,testEnv,(DataDelete)put);
    }

    @Override
    public String resultToStringDirect(String name,DataResult result) {
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
    @Override
    public String resultToKeyValueString(DataResult result) {
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

}
