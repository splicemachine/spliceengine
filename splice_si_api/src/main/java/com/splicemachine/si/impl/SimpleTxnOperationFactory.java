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

package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import static com.splicemachine.si.constants.SIConstants.*;


/**
 * @author Scott Fines
 *         Date: 7/8/14
 */
public class SimpleTxnOperationFactory implements TxnOperationFactory{
    private final ExceptionFactory exceptionLib;
    private final OperationFactory operationFactory;

    public SimpleTxnOperationFactory(ExceptionFactory exceptionFactory,
                                     OperationFactory baseFactory){
        this.exceptionLib = exceptionFactory;
        this.operationFactory = baseFactory;
    }

    @Override
    public void writeScan(DataScan scan,ObjectOutput out) throws IOException{
        operationFactory.writeScan(scan,out);
    }

    @Override
    public DataScan readScan(ObjectInput in) throws IOException{
        return operationFactory.readScan(in);
    }

    @Override
    public DataScan newDataScan(Txn txn, IsolationLevel isolationLevel){
        DataScan ds = operationFactory.newScan();
        // JL TODO Determine Server side vs. client side...
        makeNonTransactional(ds);
        return ds;
    }

    @Override
    public DataGet newDataGet(Txn txn, IsolationLevel isolationLevel, byte[] rowKey, DataGet previous){
        DataGet dg = operationFactory.newGet(rowKey,previous);
        // JL TODO Determine Server side vs. client side...
        dg.returnAllVersions();
        dg.setTimeRange(0l,Long.MAX_VALUE);
        makeNonTransactional(dg);
        return dg;
    }

    @Override
    public DataPut newDataPut(Txn txn, byte[] key, IsolationLevel isolationLevel) throws IOException{
        // JL TODO Determine Server side vs. client side...
        DataPut dp = operationFactory.newPut(key);
        makeNonTransactional(dp);
        return dp;
    }

    @Override
    public DataMutation newDataDelete(Txn txn, byte[] key,IsolationLevel isolationLevel) throws IOException{
        // JL TODO Determine Server side vs. client side...
        if(txn==null){
            return operationFactory.newDelete(key);
        }
        DataPut put = operationFactory.newPut(key);
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(),SIConstants.EMPTY_BYTE_ARRAY);
        put.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);
//        encodeForWrites(put,txn);
        return put;
    }

    @Override
    public DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        return operationFactory.newCell(key,family,qualifier,value);
    }

    @Override
    public void writeTxn(Txn txn, ObjectOutput out) throws IOException{
        out.writeObject(txn);
    }

    @Override
    public Txn readTxn(ObjectInput in) throws IOException {
        try {
            return (Txn) in.readObject();
        } catch (Exception e) {
            throw new IOException(e);
        }
    }

    protected void makeNonTransactional(Attributable op){
        op.addAttribute(SI_EXEMPT,TRUE_BYTES);
    }


}
