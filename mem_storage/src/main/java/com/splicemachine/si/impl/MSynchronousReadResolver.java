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

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.readresolve.KeyedReadResolver;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.TrafficControl;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Read-Resolver which resolves elements synchronously on the calling thread.
 *
 * @author Scott Fines
 *         Date: 7/2/14
 */
@ThreadSafe
public class MSynchronousReadResolver implements KeyedReadResolver{
    private static final Logger LOG=Logger.getLogger(MSynchronousReadResolver.class);

    //don't instantiate me, I'm a singleton!
    private MSynchronousReadResolver(){
    }

    public static final MSynchronousReadResolver INSTANCE=new MSynchronousReadResolver();
    public static volatile boolean DISABLED_ROLLFORWARD=false;

    /**
     * @param region      the region for the relevant read resolver
     * @param txnSupplier a Transaction Supplier for fetching transaction information
     * @return a ReadResolver which uses Synchronous Read Resolution under the hood.
     */
    @ThreadSafe
    public static ReadResolver getResolver(final Partition region,final TxnSupplier txnSupplier,final RollForwardStatus status){
        return getResolver(region,txnSupplier,status,null,false);
    }

    @ThreadSafe
    public static ReadResolver getResolver(final Partition region,
                                           final TxnSupplier txnSupplier,
                                           final RollForwardStatus status,
                                           final TrafficControl trafficControl,
                                           final boolean failOnError){
        return new ReadResolver(){
            @Override
            public void resolve(ByteSlice rowKey,long txnId){
                MSynchronousReadResolver.INSTANCE.resolve(region,rowKey,txnId,txnSupplier,status,failOnError,trafficControl);
            }
        };
    }

    public boolean resolve(Partition region,ByteSlice rowKey,long txnId,TxnSupplier supplier,RollForwardStatus status,boolean failOnError,TrafficControl trafficControl){
        try{
            TxnView transaction=supplier.getTransaction(txnId);
            boolean resolved=false;
            if(transaction.getEffectiveState()==Txn.State.ROLLEDBACK){
                trafficControl.acquire(1);
                try{
                    MSynchronousReadResolver.INSTANCE.resolveRolledback(region,rowKey,txnId,failOnError);
                    resolved=true;
                }finally{
                    trafficControl.release(1);
                }
            }else{
                TxnView t=transaction;
                while(t.getState()==Txn.State.COMMITTED){
                    t=t.getParentTxnView();
                }
                if(t==Txn.ROOT_TRANSACTION){
                    trafficControl.acquire(1);
                    try{
                        MSynchronousReadResolver.INSTANCE.resolveCommitted(region,rowKey,txnId,transaction.getEffectiveCommitTimestamp(),failOnError);
                        resolved=true;
                    }finally{
                        trafficControl.release(1);
                    }
                }
            }
            status.rowResolved();
            return resolved;
        }catch(IOException e){
            LOG.info("Unable to fetch transaction for id "+txnId+", will not resolve",e);
            if(failOnError)
                throw new RuntimeException(e);
            return false;
        }catch(InterruptedException e){
            LOG.debug("Interrupted which performing read resolution, will not resolve");
            Thread.currentThread().interrupt();
            return false;
        }
    }

    /******************************************************************************************************************/
    /*private helper methods */
    private void resolveCommitted(Partition region,ByteSlice rowKey,long txnId,long commitTimestamp,boolean failOnError){
        /*
         * Resolve the row as committed directly.
         *
         * This does a Put to the row, bypassing SI and the WAL, so it should be pretty low impact
         */
        if(DISABLED_ROLLFORWARD || region.isClosed() || region.isClosing())
            return; //do nothing if we are closing or rollforward is disabled

        DataPut put=new MPut(rowKey.getByteCopy());
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,txnId,
                Bytes.toBytes(commitTimestamp));
        put.addAttribute(SIConstants.SI_EXEMPT,SIConstants.TRUE_BYTES);
        put.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        try{
            region.put(put);
        }catch(IOException e){
            if(failOnError)
                throw new RuntimeException(e);
        }
    }

    private void resolveRolledback(Partition region,ByteSlice rowKey,long txnId,boolean failOnError){
        /*
         * Resolve the row as rolled back directly.
         *
         * This does a Delete to the row, bypassing SI and the WAL, so it should be pretty low impact
         */
        if(DISABLED_ROLLFORWARD || region.isClosed() || region.isClosing())
            return; //do nothing if we are closing

        DataDelete delete=new MDelete(rowKey.getByteCopy())
                .deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,txnId) //delete all the columns for our family only
                .deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txnId) //delete all the columns for our family only
                .deleteColumn(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,txnId); //delete all the columns for our family only
        delete.addAttribute(SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_NAME,SIConstants.SUPPRESS_INDEXING_ATTRIBUTE_VALUE);
        try{
            region.delete(delete);
        }catch(IOException ioe){
            LOG.info("Exception encountered when attempting to resolve a row as rolled back",ioe);
            if(failOnError)
                throw new RuntimeException(ioe);
        }
    }
}
