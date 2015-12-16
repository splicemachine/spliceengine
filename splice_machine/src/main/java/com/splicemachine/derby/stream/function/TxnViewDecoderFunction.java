package com.splicemachine.derby.stream.function;

import com.google.common.collect.Iterators;
import com.google.protobuf.ByteString;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.InheritingTxnView;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.utils.ByteSlice;
import scala.Tuple2;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by jyuan on 10/19/15.
 */
public class TxnViewDecoderFunction<Op extends SpliceOperation, T> extends SpliceFunction<Op, TxnMessage.Txn, TxnView> implements Serializable {

    private final TxnSupplier supplier;

    public TxnViewDecoderFunction() {
        super();
        this.supplier = TransactionStorage.getTxnSupplier();
    }

    public TxnViewDecoderFunction(OperationContext<Op> operationContext) {
        super(operationContext);
        this.supplier = TransactionStorage.getTxnSupplier();
    }

    @Override
    public TxnView call(TxnMessage.Txn message) throws Exception {
        TxnMessage.TxnInfo info=message.getInfo();
        if(info.getTxnId()<0) return null; //we didn't find it

        long txnId=info.getTxnId();
        long parentTxnId=info.getParentTxnid();
        long beginTs=info.getBeginTs();

        Txn.IsolationLevel isolationLevel=Txn.IsolationLevel.fromInt(info.getIsolationLevel());

        boolean hasAdditive=info.hasIsAdditive();
        boolean additive=hasAdditive && info.getIsAdditive();

        long commitTs=message.getCommitTs();
        long globalCommitTs=message.getGlobalCommitTs();

        Txn.State state=Txn.State.fromInt(message.getState());

        final Iterator<ByteSlice> destinationTables;
        if(info.hasDestinationTables()){
            ByteString bs=info.getDestinationTables();
            MultiFieldDecoder decoder=MultiFieldDecoder.wrap(bs.toByteArray());
            destinationTables=new DecodingIterator(decoder){
                @Override
                protected void advance(MultiFieldDecoder decoder){
                    decoder.skip();
                }
            };
        }else
            destinationTables= Iterators.emptyIterator();

        long kaTime=-1l;
        if(message.hasLastKeepAliveTime())
            kaTime=message.getLastKeepAliveTime();

        Iterator<ByteSlice> destTablesIterator=new Iterator<ByteSlice>(){

            @Override
            public boolean hasNext(){
                return destinationTables.hasNext();
            }

            @Override
            public ByteSlice next(){
                ByteSlice dSlice=destinationTables.next();
                byte[] data= Encoding.decodeBytesUnsortd(dSlice.array(),dSlice.offset(),dSlice.length());
                dSlice.set(data);
                return dSlice;
            }

            @Override
            public void remove(){
                throw new UnsupportedOperationException();
            }
        };

        TxnView parentTxn=parentTxnId<0?Txn.ROOT_TRANSACTION:supplier.getTransaction(parentTxnId);
        return new InheritingTxnView(parentTxn,txnId,beginTs,
                isolationLevel,
                hasAdditive,additive,
                true,true,
                commitTs,globalCommitTs,
                state,destTablesIterator,kaTime);
    }

}
