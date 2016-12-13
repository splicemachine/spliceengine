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

package com.splicemachine.compactions;

import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.*;
import org.apache.hadoop.hbase.security.User;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class SpliceDefaultCompactor extends DefaultCompactor {
    private static final boolean allowSpark = true;
    private static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);

    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        super(conf, store);
        
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "Initializing compactor: region=%s", ((HStore)this.store).getHRegion());
        }
    }

    @Override
    public List<Path> compact(CompactionRequest request,CompactionThroughputController throughputController,User user) throws IOException{
        if(store.getRegionInfo().isSystemTable())
            return super.compact(request,throughputController,user);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "compact(): request=%s", request);
        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);
        CompactionContext compactionContext=new H10Context(conf,request,this);
        long mat = 0L;
        SIDriver driver = SIDriver.driver();
        if(driver!=null && request.isAllFiles()){
            TxnRegistry.TxnRegistryView globalView = driver.getGlobalWatcher().currentView(false);
            if(globalView!=null)
                mat = globalView.getMinimumActiveTransactionId();
        }
        if(allowSpark){
            return new CompactionCoordinator(mat,store).compact(compactionContext);
        } else{
            return new CompactionProcessor(mat,store).compact(compactionContext);
        }
    }

    boolean shouldCleanSeqId(Collection<StoreFile> files,boolean allFiles) throws IOException{
        return getFileDetails(files,allFiles).minSeqIdToKeep>0;
    }

    @SuppressWarnings("UnusedParameters")
    StoreFile.Writer createTmpWriter(CompactionRequest cr,long smallestReadPoint) throws IOException{
        FileDetails fileDetails=getFileDetails(cr.getFiles(),cr.isAllFiles());
        return store.createWriterInTmp(fileDetails.maxKeyCount, this.compactionCompression, true,
                fileDetails.maxMVCCReadpoint > 0, fileDetails.maxTagsLength > 0);
    }

    public CompactionContext newContext(Collection<StoreFile> readersToClose,SparkAccumulator accumulator) throws IOException{
        CompactionRequest cr = new com.splicemachine.derby.stream.compaction.SpliceCompactionRequest(readersToClose,accumulator);
        FileDetails fd = getFileDetails(cr.getFiles(),cr.isAllFiles());
        int compactionKVMax = this.conf.getInt(HConstants.COMPACTION_KV_MAX, HConstants.COMPACTION_KV_MAX_DEFAULT);
        return new H10Context(conf,cr,this,compactionKVMax,fd.maxSeqId,fd.earliestPutTs,accumulator);
    }

    /* ****************************************************************************************************************/
    /*Abstract class overrides*/

    @Override
    protected boolean performCompaction(FileDetails fd,InternalScanner scanner,CellSink writer,long smallestReadPoint,boolean cleanSeqId,CompactionThroughputController throughputController,boolean major) throws IOException{
        CompactionContext compactionContext=newContext(Collections.emptyList(),null);
        StoreFileWriter sfw = new CellSinkWriter(writer);
        return new CompactionProcessor(0L,store).performCompaction(scanner,sfw,compactionContext);
    }


    /*
     * We have to override these so that we can expand the scope for accessibility
     */
    @Override
    public List<StoreFileScanner> createFileScanners(Collection<StoreFile> filesToCompact,long smallestReadPoint) throws IOException{
        return super.createFileScanners(filesToCompact,smallestReadPoint);
    }

    @Override
    public long getSmallestReadPoint(){
        return super.getSmallestReadPoint();
    }
}
