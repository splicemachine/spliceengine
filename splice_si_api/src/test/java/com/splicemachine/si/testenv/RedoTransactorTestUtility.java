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
import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.impl.data.UnsafeRecordUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
 *
 *
 */
public class RedoTransactorTestUtility extends TransactorTestUtility {

    public RedoTransactorTestUtility(boolean useSimple,
                                     SITestEnv testEnv,
                                     TestTransactionSetup transactorSetup) {
        super(useSimple,testEnv,transactorSetup);
    }

    @Override
    public String scan(Txn txn, String name) throws IOException {
        byte[] key = newRowKey(name);
        DataScan s = transactorSetup.txnOperationFactory.newDataScan(txn);
        s = s.startKey(key).stopKey(key).setFamily(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES);

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

    @Override
    public DataPut makePut(TestTransactionSetup transactorSetup,
                                   Txn txn,String name,int index,
                                   Object fieldValue) throws IOException {
        try {
            byte[] key = newRowKey(name);
            DataPut put = transactorSetup.txnOperationFactory.newDataPut(txn, key);

            UnsafeRecord record = new UnsafeRecord(
                    key,
                    1,
                    new byte[UnsafeRecordUtils.calculateFixedRecordSize(2)],
                    0l,true);
            record.setNumberOfColumns(2);
            record.setTxnId1(txn.getTxnId());
            if (index == 0) {
                record.setData(new int[]{0},new DataValueDescriptor[]{new SQLInteger((Integer) fieldValue)});
            } else {
                record.setData(new int[]{1},new DataValueDescriptor[]{new SQLVarchar((String) fieldValue)});
            }
            put.addCell(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES, SIConstants.PACKED_COLUMN_BYTES, 1, record.getValue());
            return put;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

    @Override
    public void deleteRowDirect(TestTransactionSetup transactorSetup,SITestEnv testEnv,
                                        Txn txn,String name) throws IOException {
        byte[] key = newRowKey(name);
        DataPut put = transactorSetup.txnOperationFactory.newDataPut(txn, key);

        UnsafeRecord record = new UnsafeRecord(
                key,
                2,
                new byte[UnsafeRecordUtils.calculateFixedRecordSize(2)],
                0l,true);
        record.setNumberOfColumns(2);
        record.setTxnId1(txn.getTxnId());
        record.setHasTombstone(true);
        put.addCell(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES, SIConstants.PACKED_COLUMN_BYTES, 2, record.getValue());
        processMutationDirect(transactorSetup,testEnv,put);
    }
    @Override
    public String resultToStringDirect(String name,DataResult result) {
        try {
            DataCell dataCell = result.iterator().next();
            UnsafeRecord record = new UnsafeRecord(dataCell.keyArray(), dataCell.keyOffset(), dataCell.keyLength(), dataCell.version(), dataCell.valueArray(), dataCell.valueOffset(), true);
            ValueRow valueRow = new ValueRow(2);
            valueRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(), new SQLVarchar()});
            record.getData(new int[]{0, 1}, valueRow);
            return name + " age=" + valueRow.getColumn(1).getString() + " job=" + valueRow.getColumn(2).getString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    @Override
     public String resultToKeyValueString(DataResult result) {
        try {
            Map<Long, String> timestampDecoder = new HashMap<>();
            final StringBuilder s = new StringBuilder();
            DataCell dataCell = result.iterator().next();
            UnsafeRecord record = new UnsafeRecord(dataCell.keyArray(), dataCell.keyOffset(), dataCell.keyLength(), dataCell.version(), dataCell.valueArray(), dataCell.valueOffset(), true);
            ValueRow valueRow = new ValueRow(2);
            valueRow.setRowArray(new DataValueDescriptor[]{new SQLInteger(), new SQLVarchar()});
            record.getData(new int[]{0, 1}, valueRow);
            if (!valueRow.isNullAt(0))
                s.append("V.age@" + timestampToStableString(timestampDecoder, dataCell.version()) + "=" + valueRow.getInt(0));
            if (!valueRow.isNullAt(1))
                s.append("V.job@" + timestampToStableString(timestampDecoder, dataCell.version()) + "=" + valueRow.getString(1));
            return s.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
