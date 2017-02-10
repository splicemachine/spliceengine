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

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationFactory;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
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
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(data,off,length);
        long txnId=decoder.decodeNextLong();
        long beginTs=decoder.decodeNextLong();
        boolean additive=decoder.decodeNextBoolean();
        Txn.IsolationLevel level=Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        //throw away the allow reads bit, since we won't care anyway
        decoder.decodeNextBoolean();

        TxnView parent=Txn.ROOT_TRANSACTION;
        while(decoder.available()){
            long id=decoder.decodeNextLong();
            parent=new ActiveWriteTxn(id,id,parent,additive,level);
        }
        return new ActiveWriteTxn(txnId,beginTs,parent,additive,level);
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
        MultiFieldEncoder encoder=MultiFieldEncoder.create(6)
                .encodeNext(txn.getTxnId())
                .encodeNext(txn.getBeginTimestamp())
                .encodeNext(txn.isAdditive())
                .encodeNext(txn.getIsolationLevel().encode())
                .encodeNext(txn.allowsWrites());

        LongArrayList parentTxnIds=LongArrayList.newInstance();
        byte[] build=encodeParentIds(txn,parentTxnIds);
        encoder.setRawBytes(build);
        return encoder.build();
    }

    public TxnView decode(byte[] data,int offset,int length){
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(data,offset,length);
        long txnId=decoder.decodeNextLong();
        long beginTs=decoder.decodeNextLong();
        boolean additive=decoder.decodeNextBoolean();
        Txn.IsolationLevel level=Txn.IsolationLevel.fromByte(decoder.decodeNextByte());
        boolean allowsWrites=decoder.decodeNextBoolean();

        TxnView parent=Txn.ROOT_TRANSACTION;
        while(decoder.available()){
            long id=decoder.decodeNextLong();
            if(allowsWrites)
                parent=new ActiveWriteTxn(id,id,parent,additive,level);
            else
                parent=new ReadOnlyTxn(id,id,level,parent,UnsupportedLifecycleManager.INSTANCE,exceptionLib,additive);
        }
        if(allowsWrites)
            return new ActiveWriteTxn(txnId,beginTs,parent,additive,level);
        else
            return new ReadOnlyTxn(txnId,beginTs,level,parent,UnsupportedLifecycleManager.INSTANCE,exceptionLib,additive);
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


    private byte[] encodeParentIds(TxnView txn,LongArrayList parentTxnIds){
        /*
         * For both active reads AND active writes, we only need to know the
         * parent's transaction ids, since we'll use the information immediately
         * available to determine other properties (additivity, etc.) Thus,
         * by doing this bit of logic, we can avoid a network call on the server
         * for every parent on the transaction chain, at the cost of 2-10 bytes
         * per parent on the chain--a cheap trade.
         */
        TxnView parent=txn.getParentTxnView();
        while(!Txn.ROOT_TRANSACTION.equals(parent)){
            parentTxnIds.add(parent.getTxnId());
            parent=parent.getParentTxnView();
        }
        int parentSize=parentTxnIds.size();
        long[] parentIds=parentTxnIds.buffer;
        MultiFieldEncoder parents=MultiFieldEncoder.create(parentSize);
        for(int i=1;i<=parentSize;i++){
            parents.encodeNext(parentIds[parentSize-i]);
        }
        return parents.build();
    }


}
