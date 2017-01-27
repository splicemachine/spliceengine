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

package com.splicemachine.derby.stream.function;

import org.spark_project.guava.collect.Iterators;
import com.google.protobuf.ByteString;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.encoding.DecodingIterator;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.InheritingTxnView;
import com.splicemachine.utils.ByteSlice;

import java.io.Serializable;
import java.util.Iterator;

/**
 * Created by jyuan on 10/19/15.
 */
public class TxnViewDecoderFunction<Op extends SpliceOperation, T> extends SpliceFunction<Op, TxnMessage.Txn, TxnView> implements Serializable {

    private final TxnSupplier supplier;

    public TxnViewDecoderFunction() {
        super();
        this.supplier = SIDriver.driver().getTxnSupplier();
    }

    public TxnViewDecoderFunction(OperationContext<Op> operationContext) {
        super(operationContext);
        this.supplier = SIDriver.driver().getTxnSupplier();
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
