package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.ActiveWriteTxn;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.storage.Attributable;

import java.io.IOException;
import java.io.ObjectInput;

import static com.splicemachine.si.constants.SIConstants.*;


/**
 * @author Scott Fines
 *         Date: 7/8/14
 */
public abstract class BaseOperationFactory<OperationWithAttributes,
        Data,
        Delete extends OperationWithAttributes,
        Filter,
        Get extends OperationWithAttributes,
        Mutation extends OperationWithAttributes,
        Put extends OperationWithAttributes,
        RegionScanner,
        Result,
        Scan extends OperationWithAttributes> implements TxnOperationFactory<OperationWithAttributes, Get, Mutation, Put, Scan>{
    private SDataLib<OperationWithAttributes, Data, Delete, Filter, Get, Put, RegionScanner, Result, Scan> dataLib;
    private ExceptionFactory exceptionLib;

    public BaseOperationFactory(SDataLib<OperationWithAttributes, Data, Delete, Filter, Get, Put, RegionScanner, Result, Scan> dataLib,
                                ExceptionFactory exceptionFactory){
        this.dataLib = dataLib;
        this.exceptionLib = exceptionFactory;
    }

    @Override
    public Put newPut(TxnView txn,byte[] rowKey) throws IOException{
        Put put=dataLib.newPut(rowKey);
        if(!txn.allowsWrites())
            throw exceptionLib.readOnlyModification("transaction is read only: "+txn.getTxnId());
        encodeForWrites(put,txn);
        return put;
    }


    @Override
    public Scan newScan(TxnView txn){
        return newScan(txn,false);
    }

    @Override
    public Scan newScan(TxnView txn,boolean isCountStar){
        Scan scan=dataLib.newScan();
        if(txn==null){
            makeNonTransactional(scan);
            return scan;
        }
        encodeForReads(scan,txn,isCountStar);
        return scan;
    }

    @Override
    public Get newGet(TxnView txn,byte[] rowKey){
        Get get=dataLib.newGet(rowKey);
        if(txn==null){
            makeNonTransactional(get);
            return get;
        }
        encodeForReads(get,txn,false);
        return get;
    }

    @Override
    public Mutation newDelete(TxnView txn,byte[] rowKey) throws IOException{
        if(txn==null){
            Delete delete=dataLib.newDelete(rowKey);
            makeNonTransactional(delete);
            return (Mutation)delete;
        }
        Put delete=dataLib.newPut(rowKey);
        dataLib.setAttribute(delete,SI_DELETE_PUT,TRUE_BYTES);
        dataLib.addKeyValueToPut(delete,DEFAULT_FAMILY_BYTES,SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(),EMPTY_BYTE_ARRAY);
        if(!txn.allowsWrites())
            throw exceptionLib.readOnlyModification("transaction is read only: "+txn.getTxnId());
        encodeForWrites(delete,txn);
        return (Mutation)delete;
    }

    @Override
    public TxnView fromReads(OperationWithAttributes op) throws IOException{
        byte[] txnData=dataLib.getAttribute(op,SI_TRANSACTION_ID_KEY);
        if(txnData==null) return null; //non-transactional
        return decode(txnData,0,txnData.length);
    }


    @Override
    public TxnView fromWrites(OperationWithAttributes op) throws IOException{
        byte[] txnData=dataLib.getAttribute(op,SI_TRANSACTION_ID_KEY);
        if(txnData==null) return null; //non-transactional
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(txnData);
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
        return new ActiveWriteTxn(beginTs,beginTs,parent,additive,level);
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
        MultiFieldDecoder decoder=MultiFieldDecoder.wrap(txnData);
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
        return new ActiveWriteTxn(beginTs,beginTs,parent,additive,level);
    }

    @Override
    public TxnView readTxn(ObjectInput oi) throws IOException{
        int size=oi.readInt();
        byte[] txnData=new byte[size];
        oi.read(txnData);

        return decode(txnData,0,txnData.length);
    }

    @Override
    public byte[] encode(TxnView txn){
        MultiFieldEncoder encoder=MultiFieldEncoder.create(5)
                .encodeNext(txn.getTxnId())
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
                parent=new ReadOnlyTxn(id,id,level,parent,UnsupportedLifecycleManager.INSTANCE,additive);
        }
        if(allowsWrites)
            return new ActiveWriteTxn(beginTs,beginTs,parent,additive,level);
        else
            return new ReadOnlyTxn(beginTs,beginTs,level,parent,UnsupportedLifecycleManager.INSTANCE,additive);
    }

    /******************************************************************************************************************/
        /*private helper functions*/
    private void encodeForWrites(OperationWithAttributes op,TxnView txn){
        byte[] data=encode(txn);
        dataLib.setAttribute(op,SI_TRANSACTION_ID_KEY,data);
        dataLib.setAttribute(op,SI_NEEDED,SI_NEEDED_VALUE_BYTES);
    }

    private void encodeForReads(OperationWithAttributes op,TxnView txn,boolean isCountStar){
        if(isCountStar)
            dataLib.setAttribute(op,SI_COUNT_STAR,TRUE_BYTES);
        byte[] data=encode(txn);
        dataLib.setAttribute(op,SI_TRANSACTION_ID_KEY,data);
        dataLib.setAttribute(op,SI_NEEDED,SI_NEEDED_VALUE_BYTES);
    }

    protected void encodeForWrites(Attributable op,TxnView txn){
        byte[] data=encode(txn);
        op.addAttribute(SI_TRANSACTION_ID_KEY,data);
        op.addAttribute(SI_NEEDED,SI_NEEDED_VALUE_BYTES);
    }

    protected void encodeForReads(Attributable op,TxnView txn,boolean isCountStar){
        if(isCountStar)
            op.addAttribute(SI_COUNT_STAR,TRUE_BYTES);
        byte[] data=encode(txn);
        op.addAttribute(SI_TRANSACTION_ID_KEY,data);
        op.addAttribute(SI_NEEDED,SI_NEEDED_VALUE_BYTES);
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


    private void makeNonTransactional(OperationWithAttributes op){
        dataLib.setAttribute(op,SI_EXEMPT,TRUE_BYTES);
    }


}
