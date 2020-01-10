/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.storage.Partition;

/**
 * @author Scott Fines
 *         Date: 1/29/14
 */
public class ForwardRecordingCallBuffer<E> implements RecordingCallBuffer<E>{
    protected final RecordingCallBuffer<E> delegate;

    public ForwardRecordingCallBuffer(RecordingCallBuffer<E> delegate){
        this.delegate=delegate;
    }

    @Override
    public void add(E element) throws Exception{
        delegate.add(element);
    }

    @Override
    public void addAll(E[] elements) throws Exception{
        delegate.addAll(elements);
    }

    @Override
    public void addAll(Iterable<E> elements) throws Exception{
        delegate.addAll(elements);
    }

    @Override
    public void flushBuffer() throws Exception{
        delegate.flushBuffer();
    }

    @Override
    public void flushBufferAndWait() throws Exception{
        delegate.flushBufferAndWait();
    }

    @Override
    public void close() throws Exception{
        delegate.close();
    }

    @Override
    public long getTotalElementsAdded(){
        return delegate.getTotalElementsAdded();
    }

    @Override
    public long getTotalBytesAdded(){
        return delegate.getTotalBytesAdded();
    }

    @Override
    public long getTotalFlushes(){
        return delegate.getTotalFlushes();
    }

    @Override
    public double getAverageEntriesPerFlush(){
        return delegate.getAverageEntriesPerFlush();
    }

    @Override
    public double getAverageSizePerFlush(){
        return delegate.getAverageSizePerFlush();
    }

    @Override
    public CallBuffer<E> unwrap(){
        return delegate.unwrap();
    }

    @Override
    public WriteStats getWriteStats(){
        return delegate.getWriteStats();
    }

    @Override
    public PreFlushHook getPreFlushHook(){
        return delegate.getPreFlushHook();
    }

    @Override
    public WriteConfiguration getWriteConfiguration(){
        return delegate.getWriteConfiguration();
    }

    @Override
    public TxnView getTxn(){
        return delegate.getTxn();
    }

    @Override
    public Partition destinationPartition(){
        return delegate.destinationPartition();
    }

    @Override
    public E lastElement(){
        return delegate.lastElement();
    }
}
