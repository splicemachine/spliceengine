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
 *
 */

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongOpenHashSet;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.api.readresolve.ReadResolver;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.store.ActiveTxnCacheSupplier;
import com.splicemachine.si.impl.txn.CommittedTxn;
import com.splicemachine.storage.CellType;
import com.splicemachine.storage.DataCell;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;

/**
 * Transaction filter which performs basic transactional filtering (i.e. row visibility, tombstones,
 * anti-tombstones, etc.)
 *
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class NonSITxnFilter implements TxnFilter{
    //per row fields
    private final LongOpenHashSet visitedTxnIds=new LongOpenHashSet();
    private final LongArrayList tombstonedTxnRows=new LongArrayList(1); //usually, there are very few deletes
    private final LongArrayList antiTombstonedTxnRows=new LongArrayList(1);
    private final ByteSlice rowKey=new ByteSlice();

    @SuppressWarnings("unchecked")
    public NonSITxnFilter(){
    }

    @Override
    public boolean filterRow(){
        return getExcludeRow();
    }

    @Override
    public void reset(){
        nextRow();
    }

    @Override
    public ReturnCode filterCell(DataCell keyValue) throws IOException{
        CellType type=keyValue.dataType();
        if(type==CellType.COMMIT_TIMESTAMP){
            return ReturnCode.SKIP;
        }
        if(type==CellType.FOREIGN_KEY_COUNTER){
            /* Transactional reads always ignore this column, no exceptions. */
            return ReturnCode.SKIP;
        }

        switch(type){
            case TOMBSTONE:
                addToTombstoneCache(keyValue);
                return ReturnCode.SKIP;
            case ANTI_TOMBSTONE:
                addToAntiTombstoneCache(keyValue);
                return ReturnCode.SKIP;
            case USER_DATA:
                return checkVisibility(keyValue);
            default:
                //TODO -sf- do better with this?
                throw new AssertionError("Unexpected Data type: "+type);
        }
    }

    @Override
    public DataCell produceAccumulatedResult(){
        return null;
    }

    @Override
    public void nextRow(){
        //clear row-specific fields
        visitedTxnIds.clear();
        tombstonedTxnRows.clear();
        antiTombstonedTxnRows.clear();
        rowKey.reset();
    }

    @Override
    public boolean getExcludeRow(){
        return false;
    }




    private ReturnCode checkVisibility(DataCell data) throws IOException{
        return ReturnCode.INCLUDE;
    }

    private boolean isVisible(long txnId) throws IOException{
        return true;
    }

    private void addToAntiTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();
        if(isVisible(txnId)){
			/*
			 * We can see this anti-tombstone, hooray!
			 */
            if(!tombstonedTxnRows.contains(txnId))
                antiTombstonedTxnRows.add(txnId);
        }
    }

    private void addToTombstoneCache(DataCell data) throws IOException{
        long txnId=data.version();//this.dataStore.getOpFactory().getTimestamp(data);
		/*
		 * Only add a tombstone to our list if it's actually visible,
		 * otherwise there's no point, since we can't see it anyway.
		 */
        if(isVisible(txnId)){
            if(!antiTombstonedTxnRows.contains(txnId))
                tombstonedTxnRows.add(txnId);
        }
    }


    @Override
    public RowAccumulator getAccumulator(){
        throw new RuntimeException("not implemented");
    }
}
