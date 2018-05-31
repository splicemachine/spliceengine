/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.carrotsearch.hppc.cursors.LongCursor;
import com.google.protobuf.InvalidProtocolBufferException;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.storage.*;

import java.util.Collections;
import java.util.List;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;

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
    public DataScan newDataScan(TxnView txn){
        DataScan ds = operationFactory.newScan();
        if(txn!=null)
            encodeForReads(ds,txn,false);
        else
            makeNonTransactional(ds);
        return ds;
    }

    @Override
    public DataGet newDataGet(TxnView txn,byte[] rowKey,DataGet previous){
        DataGet dg = operationFactory.newGet(rowKey,previous);
        dg.returnAllVersions();
        dg.setTimeRange(0l,Long.MAX_VALUE);
        if(txn!=null){
            encodeForReads(dg,txn,false);
        }else
            makeNonTransactional(dg);

        return dg;
    }

    @Override
    public DataPut newDataPut(TxnView txn,byte[] key) throws IOException{
        DataPut dp = operationFactory.newPut(key);
        if(txn==null){
            makeNonTransactional(dp);
        }else
            encodeForWrites(dp,txn);
        return dp;
    }

    @Override
    public DataMutation newDataDelete(TxnView txn,byte[] key) throws IOException{
        if(txn==null){
            return operationFactory.newDelete(key);
        }
        DataPut put = operationFactory.newPut(key);
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(),SIConstants.EMPTY_BYTE_ARRAY);
        put.addAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);
        encodeForWrites(put,txn);
        return put;
    }

    @Override
    public DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value){
        return operationFactory.newCell(key,family,qualifier,value);
    }

    @Override
    public TxnView fromReads(Attributable op) throws IOException{
        byte[] txnData = op.getAttribute(SI_TRANSACTION_ID_KEY);
        if(txnData==null) return null;
        return decode(txnData,0,txnData.length);
    }

    @Override
    public TxnView fromWrites(Attributable op) throws IOException{
        byte[] txnData=op.getAttribute(SI_TRANSACTION_ID_KEY);
        if(txnData==null) return null; //non-transactional
        return fromWrites(txnData,0,txnData.length);
    }

    @Override
    public TxnView fromWrites(byte[] data,int off,int length) throws IOException{
        if(length<=0) return null; //non-transactional
        return decode(data,off,length);
    }

    @Override
    public TxnView fromReads(byte[] data,int off,int length) throws IOException{
        return decode(data,off,length);
    }

    @Override
    public TxnView readTxn(ObjectInput oi) throws IOException{
        int size=oi.readInt();
        byte[] txnData=new byte[size];
        oi.readFully(txnData);

        return decode(txnData,0,txnData.length);
    }

    @Override
    public byte[] encode(TxnView txn){
        List parentIds = new ArrayList<>();
        TxnView parent=txn.getParentTxnView();
        while(!Txn.ROOT_TRANSACTION.equals(parent)){
            parentIds.add(parent.getTxnId());
            parent=parent.getParentTxnView();
        }
        //TxnView txnToEncode= txn;
        TxnView txnToEncode= null;
        // Code is added to allow propogation of rolledback sub txns that will not require a
        // lookup on the read side.
        if (txn.getParentTxnView() != null  && txn.getTxnId() == txn.getParentTxnView().getTxnId()) {
            txnToEncode = txn.getParentTxnView();
        } else {
            txnToEncode = txn;
        }

        TxnMessage.TxnInfo.Builder builder = TxnMessage.TxnInfo.newBuilder()
                .setTxnId(txnToEncode.getTxnId())
                .setBeginTs(txnToEncode.getBeginTimestamp())
                // There can be changes for isAdditive and you have to use the original
                // for constant iteration.
                .setIsAdditive(txn.isAdditive())
                .setIsolationLevel(txnToEncode.getIsolationLevel().encode())
                .setAllowsWrites(txnToEncode.allowsWrites())
                .addAllParentIds(parentIds);
        if (txnToEncode.getRolledback() != null) {
            LongOpenHashSet hashSet = txnToEncode.getRolledback();
            ArrayList list = new ArrayList(hashSet.size());
            long[] values = hashSet.toArray();
            for (int i = 0; i< hashSet.size(); i++) {
                list.add(values[i]);
            }
            builder.addAllRollbackSubIds(list);
        }
        return builder.build().toByteArray();
    }

    public TxnView decode(byte[] data,int offset,int length){
        TxnMessage.TxnInfo info = null;
        try {
            info = TxnMessage.TxnInfo.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(e);
        }
        Txn.IsolationLevel isolationLevel = Txn.IsolationLevel.fromByte((byte)info.getIsolationLevel());
        TxnView parent=Txn.ROOT_TRANSACTION;
        LongOpenHashSet rolledBackIds = null;
        if (info.getRollbackSubIdsCount() > 0) {
            rolledBackIds = new LongOpenHashSet(info.getRollbackSubIdsCount());
            for (Long rolledBackId: info.getRollbackSubIdsList()) {
                rolledBackIds.add(rolledBackId);
            }
        }
        for (int i = 1; i <= info.getParentIdsCount(); i++) {
            long id = info.getParentIds(info.getParentIdsCount()-i);
            if(info.getAllowsWrites())
                parent=new ActiveWriteTxn(id,id,parent,info.getIsAdditive(), isolationLevel,rolledBackIds);
            else
                parent=new ReadOnlyTxn(id,id,isolationLevel,parent,UnsupportedLifecycleManager.INSTANCE,exceptionLib,info.getIsAdditive());
        }
        if(info.getAllowsWrites())
            return new ActiveWriteTxn(info.getTxnId(),info.getBeginTs(),parent,info.getIsAdditive(),isolationLevel,rolledBackIds);
        else
            return new ReadOnlyTxn(info.getTxnId(),info.getBeginTs(),isolationLevel,parent,UnsupportedLifecycleManager.INSTANCE,exceptionLib,info.getIsAdditive());
    }

    @Override
    public void writeTxn(TxnView txn,ObjectOutput out) throws IOException{
        byte[] eData= encode(txn);
        out.writeInt(eData.length);
        out.write(eData,0,eData.length);
    }

    @Override
    public void encodeForWrites(Attributable op,TxnView txn) throws IOException{
        if(!txn.allowsWrites())
            throw exceptionLib.readOnlyModification("ReadOnly txn "+txn.getTxnId());
        byte[] data=encode(txn);
        op.addAttribute(SI_TRANSACTION_ID_KEY,data);
        op.addAttribute(SI_NEEDED,SI_NEEDED_VALUE_BYTES);
    }

    @Override
    public void encodeForReads(Attributable op,TxnView txn,boolean isCountStar){
        if(isCountStar)
            op.addAttribute(SI_COUNT_STAR,TRUE_BYTES);
        byte[] data=encode(txn);
        op.addAttribute(SI_TRANSACTION_ID_KEY,data);
        op.addAttribute(SI_NEEDED,SI_NEEDED_VALUE_BYTES);
    }

    protected void makeNonTransactional(Attributable op){
        op.addAttribute(SI_EXEMPT,TRUE_BYTES);
    }


}
