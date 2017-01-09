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

package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.Txn;
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
    public Txn getTxn(){
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
