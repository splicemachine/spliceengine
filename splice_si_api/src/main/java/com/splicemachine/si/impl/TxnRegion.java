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

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.readresolve.RollForward;
import com.splicemachine.si.api.server.ConstraintChecker;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.server.Transactor;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.filter.HRowAccumulator;
import com.splicemachine.si.impl.filter.PackedTxnFilter;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import org.spark_project.guava.collect.Iterators;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

/**
 * Base implementation of a TransactionalRegion
 *
 * @author Scott Fines
 *         Date: 7/1/14
 */
public class TxnRegion<InternalScanner> implements TransactionalRegion<InternalScanner>{
    private final RollForward rollForward;
    private final ReadResolver readResolver;
    private final TxnSupplier txnSupplier;
    private final Transactor transactor;
    private final TxnOperationFactory opFactory;
    private Partition region;
    private String tableName;


    public TxnRegion(Partition region,
                     RollForward rollForward,
                     ReadResolver readResolver,
                     TxnSupplier txnSupplier,
                     Transactor transactor,TxnOperationFactory opFactory){
        this.region=region;
        this.rollForward=rollForward;
        this.readResolver=readResolver;
        this.txnSupplier=txnSupplier;
        this.transactor=transactor;
        this.opFactory=opFactory;
        if(region!=null){
            this.tableName=region.getTableName();
        }
    }

    @Override
    public TxnFilter unpackedFilter(TxnView txn) throws IOException{
        return new SimpleTxnFilter(tableName,txn,readResolver,txnSupplier);
    }

    @Override
    public TxnFilter packedFilter(TxnView txn,EntryPredicateFilter predicateFilter,boolean countStar) throws IOException{
        return new PackedTxnFilter(unpackedFilter(txn),new HRowAccumulator(predicateFilter,new EntryDecoder(),countStar));
    }

//    @Override
//    public SICompactionState compactionFilter() throws IOException{
//        throw new UnsupportedOperationException("IMPLEMENT");
//    }

    @Override
    public InternalScanner compactionScanner(InternalScanner internalScanner){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean rowInRange(byte[] row){
        return region.containsRow(row);
    }

    @Override
    public boolean rowInRange(ByteSlice slice){
        return region.containsRow(slice.array(),slice.offset(),slice.length());
    }

    @Override
    public boolean isClosed(){
        return region.isClosed() || region.isClosing();
    }

    @Override
    public String getTableName(){
        return tableName;
    }

    @Override
    public void updateWriteRequests(long writeRequests){
        region.writesRequested(writeRequests);
    }

    @Override
    public Iterable<MutationStatus> bulkWrite(TxnView txn,
                                       byte[] family,byte[] qualifier,
                                       ConstraintChecker constraintChecker, //TODO -sf- can we encapsulate this as well?
                                       Collection<KVPair> data) throws IOException{
        /*
         * Designed for subclasses. Override this if you want to bypass transactional writes
         */
        final MutationStatus[] status = transactor.processKvBatch(region, rollForward, family, qualifier, data,txn,constraintChecker);
        return new Iterable<MutationStatus>(){
            @Override public Iterator<MutationStatus> iterator(){ return Iterators.forArray(status); }
        };
    }

    @Override
    public String getRegionName(){
        return region.getName();
    }

    @Override
    public TxnSupplier getTxnSupplier(){
        return txnSupplier;
    }

    @Override
    public ReadResolver getReadResolver(){
        return readResolver;
    }

    @Override
    public void close(){
    } //no-op

    @Override
    public Partition unwrap(){
        return region;
    }

    @Override
    public String toString(){
        return region.getName();
    }
}
