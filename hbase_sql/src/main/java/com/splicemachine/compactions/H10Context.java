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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.regionserver.StoreFile;
import org.apache.hadoop.hbase.regionserver.StoreFileScanner;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Scott Fines
 *         Date: 12/8/16
 */
public class H10Context extends BaseCompactionContext{
    private final SpliceDefaultCompactor sourceCompactor;
    private final Configuration conf;

    private Collection<StoreFile> readersToClose;
    private ProgressWatcher watcher;

    private long smallestReadPoint = -1L;

    public H10Context(Configuration conf,
                      CompactionRequest request,
                      SpliceDefaultCompactor sourceCompactor,
                      com.splicemachine.si.impl.server.Compactor.Accumulator accumulator){
        this(conf,request,sourceCompactor,-1,-1L,-1L,accumulator);
    }

    public H10Context(Configuration conf,
                      CompactionRequest request,
                      SpliceDefaultCompactor sourceCompactor,
                      int compactionKVMax,
                      long maxSeqId,
                      long earliestPutTs,
                      com.splicemachine.si.impl.server.Compactor.Accumulator accumulator){
        super(request,compactionKVMax,maxSeqId,earliestPutTs,accumulator);
        this.conf = conf;
        this.sourceCompactor = sourceCompactor;
    }

    public H10Context(Configuration conf,
                      CompactionRequest request,
                      SpliceDefaultCompactor sourceCompactor){
        this(conf,request,sourceCompactor,null);
    }

    @Override
    public long getSmallestReadPoint(){
        if(smallestReadPoint<0)
            smallestReadPoint = sourceCompactor.getSmallestReadPoint();
        return smallestReadPoint;
    }

    @Override
    public void cancel(){
        CompactionProgress progress=sourceCompactor.getProgress();
        if(progress!=null)
            progress.cancel();
    }

    @Override
    public void complete(){
        CompactionProgress progress=sourceCompactor.getProgress();
        if(progress!=null)
            progress.complete();
    }

    @Override
    public ProgressWatcher progressWatcher(){
        if(watcher==null){
            final CompactionProgress progress=sourceCompactor.getProgress();
            if(progress!=null){
                watcher = new ProgressWatcher(){
                    @Override
                    public void incrementCompactedKVs(){
                        progress.currentCompactedKVs++;
                    }

                    @Override
                    public void incrementCompactedSize(long add){
                        progress.totalCompactedSize+=add;
                    }

                    @Override
                    public void markComplete(){
                        progress.complete();
                    }
                };
            }else{
                com.splicemachine.si.impl.server.Compactor.Accumulator accumulator=accumulator();
                if(accumulator!=null){
                    watcher = new ProgressWatcher(){
                        @Override
                        public void incrementCompactedKVs(){
                            accumulator.kvCompacted();
                        }

                        @Override
                        public void incrementCompactedSize(long add){
                            System.out.printf("Accumulating %d to compaction size%n",add);
                            accumulator.incrementCompactionSize(add);
                        }

                        @Override public void markComplete(){ }
                    };
                }else{
                    watcher = new ProgressWatcher(){
                        @Override public void incrementCompactedKVs(){ }
                        @Override public void incrementCompactedSize(long add){ }
                        @Override public void markComplete(){ }
                    };
                }
            }
        }

        return watcher;
    }

    @Override
    public boolean retainDeleteMarkers(){
        //-sf- this may not be correct in all cases...
        return !request.isAllFiles();
    }

    @Override
    public boolean shouldCleanSeqId() throws IOException{
        return sourceCompactor.shouldCleanSeqId(request.getFiles(),request.isAllFiles());
    }

    @Override
    public StoreFileWriter createTmpWriter() throws IOException{
        return new BasicStoreFileWriter(sourceCompactor.createTmpWriter(request,getSmallestReadPoint()));
    }

    @Override
    public List<StoreFileScanner> createFileScanners() throws IOException{
        if(conf.getBoolean("hbase.regionserver.compaction.private.readers",false)){
            readersToClose = new ArrayList<>(request.getFiles().size());
            readersToClose.addAll(request.getFiles().stream()
                    .map(StoreFile::new)
                    .collect(Collectors.toList()));
            return sourceCompactor.createFileScanners(readersToClose,getSmallestReadPoint());
        }else {
            readersToClose = Collections.emptyList();
            return sourceCompactor.createFileScanners(request.getFiles(),getSmallestReadPoint());
        }
    }

    @Override
    public void closeStoreFileReaders(boolean b) throws IOException{
        IOException error = null;
        if(readersToClose==null) return;

        for(StoreFile f:readersToClose){
            try{
                f.closeReader(b);

            }catch(IOException e){
                if(error==null) error =e;
                else error.addSuppressed(e);
            }
        }
        if(error!=null)
            throw error;
    }
}
