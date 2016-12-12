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
 *
 */

package com.splicemachine.compactions;

import com.splicemachine.si.impl.server.Compactor;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Scott Fines
 *         Date: 12/8/16
 */
public abstract class BaseCompactionContext implements CompactionContext{
    protected final CompactionRequest request;
    private final int compactionKVMax;
    private final long maxSeqId;
    private final long earliestPutTs;
    private final Compactor.Accumulator accumulator;

    public BaseCompactionContext(CompactionRequest request,
                                 int compactionKVMax,
                                 long maxSeqId,
                                 long earliestPutTs,
                                 Compactor.Accumulator accumulator){
        this.request=request;
        this.compactionKVMax=compactionKVMax;
        this.maxSeqId=maxSeqId;
        this.earliestPutTs=earliestPutTs;
        this.accumulator=accumulator;
    }

    @Override
    public List<String> getFilePaths(){
        return request.getFiles().stream().map(file -> file.getPath().toString()).collect(Collectors.toList());
    }

    @Override
    public long fileSize(){
        return request.getSize();
    }

    @Override
    public long getSelectionTime(){
        return request.getSelectionTime();
    }

    @Override
    public boolean isMajor(){
        return request.isMajor();
    }

    @Override
    public boolean isAllFiles(){
        return request.isAllFiles();
    }

    @Override
    public int compactionKVMax(){
        return compactionKVMax;
    }

    @Override
    public long maxSeqId(){
        return maxSeqId;
    }

    @Override
    public boolean retainDeleteMarkers(){
        return request.isRetainDeleteMarkers();
    }

    @Override
    public long earliestPutTs(){
        return earliestPutTs;
    }

    @Override
    public Compactor.Accumulator accumulator(){
        return accumulator;
    }
}
