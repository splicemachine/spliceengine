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

import com.splicemachine.access.HConfiguration;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.Compactor;
import com.splicemachine.si.impl.server.MVCCCompactor;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Performs the actual reading/writing of Compaction
 * files.
 *
 * This is abstracted away to provide shared logic for all versions of HBase.
 *
 * @author Scott Fines
 *         Date: 12/8/16
 */
public class CompactionProcessor{
    private static final Logger LOG = Logger.getLogger(CompactionProcessor.class);

    private final long mat;
    private final Store store;

    public CompactionProcessor(long mat,Store store){
        this.mat=mat;
        this.store=store;
    }

    public List<Path> compact(CompactionContext context) throws IOException{
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "sparkCompact(): CompactionRequest=%s", context);

        List<StoreFileScanner> scanners=context.createFileScanners();

        List<Path> newFiles =new ArrayList<>();
        try {
            /* Include deletes, unless we are doing a compaction of all files */
            ScanType scanType = context.retainDeleteMarkers() ? ScanType.COMPACT_RETAIN_DELETES
                    : ScanType.COMPACT_DROP_DELETES;
            try(InternalScanner blockScanner =  createScanner(context, store,scanners, scanType)){
                InternalScanner scanner = blockScanner;

                if (needsSI(store.getTableName())) {
                    SIDriver driver=SIDriver.driver();
                    Compactor.Accumulator sparkAccumulator = context.accumulator();
                    com.splicemachine.si.impl.server.Compactor state= new MVCCCompactor(driver.getTxnSupplier(),
                            mat,
                            64,
                            driver.getConfiguration().getActiveTransactionCacheSize(),
                            sparkAccumulator);
                    scanner = new SICompactionScanner(state,scanner);
                }
                if (scanner == null) {
                    // NULL scanner returned from coprocessor hooks means skip normal processing.
                    return newFiles;
                }

                try(StoreFileWriter writer = context.createTmpWriter()){
                    boolean finished=performCompaction(scanner,writer,context);
                    if(!finished){
                        writer.close();
                        store.getFileSystem().delete(writer.getPath(),false);
                        throw new InterruptedIOException("Aborting compaction of store "+store+
                                " in region "+store.getRegionInfo().getRegionNameAsString()+
                                " because it was interrupted.");
                    }
                }
            }
        }finally {
            try{
                context.closeStoreFileReaders(true);
            }catch(IOException ioe){
                LOG.warn("Exception closing store file readers", ioe);
            }
        }
        return newFiles;
    }


    public boolean performCompaction(InternalScanner scanner,StoreFileWriter writer,CompactionContext context) throws IOException{
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"performCompaction");
        long bytesWritten = 0;
        long bytesWrittenProgress = 0;

        // Since scanner.next() can return 'false' but still be delivering data,
        // we have to use a do/while loop.
        List<Cell> cells =new ArrayList<>();
        long closeCheckInterval = HStore.getCloseCheckInterval();
        long lastMillis = 0;
        if (LOG.isDebugEnabled()) {
            lastMillis = EnvironmentEdgeManager.currentTime();
        }
        long now = 0;
        boolean hasMore;
        ScannerContext scannerContext = ScannerContext.newBuilder().setBatchLimit(context.compactionKVMax()).build();
        long smallestReadPoint = context.getSmallestReadPoint();
        boolean cleanSeqId = context.shouldCleanSeqId();
        CompactionContext.ProgressWatcher pw = context.progressWatcher();
        do {
            hasMore = scanner.next(cells, scannerContext);
            if (LOG.isDebugEnabled()) {
                now = EnvironmentEdgeManager.currentTime();
            }
            // output to writer:
            for (Cell c : cells) {
                if (cleanSeqId && c.getSequenceId() <= smallestReadPoint) {
                    CellUtil.setSequenceId(c, 0);
                }
                writer.append(c);
                int len = KeyValueUtil.length(c);
                pw.incrementCompactedKVs();
                pw.incrementCompactedSize(len);
                if (LOG.isDebugEnabled()) {
                    bytesWrittenProgress += len;
                }
                // check periodically to see if a system stop is requested
                if (closeCheckInterval > 0) {
                    bytesWritten += len;
                    if (bytesWritten > closeCheckInterval) {
                        bytesWritten = 0;
                    }
                }
            }
            // Log the progress of long running compactions every minute if
            // logging at DEBUG level
            if (LOG.isDebugEnabled()) {
                if ((now - lastMillis) >= 60 * 1000) {
                    LOG.debug("Compaction progress: " + pw + String.format(", rate=%.2f kB/sec",
                            (bytesWrittenProgress / 1024.0) / ((now - lastMillis) / 1000.0)));
                    lastMillis = now;
                    bytesWrittenProgress = 0;
                }
            }
            cells.clear();
        } while (hasMore);
        pw.markComplete();
        return true;
    }


    /* ****************************************************************************************************************/
    /*private helper methods*/
    private InternalScanner createScanner(CompactionContext context,Store store, List<StoreFileScanner> scanners, ScanType scanType) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        return new StoreScanner(store, store.getScanInfo(), scan, scanners, scanType, context.getSmallestReadPoint(), context.earliestPutTs());
    }

    private boolean needsSI(TableName tableName) {
        TableType type = EnvUtils.getTableType(HConfiguration.getConfiguration(), tableName);
        switch (type) {
            case TRANSACTION_TABLE:
            case ROOT_TABLE:
            case META_TABLE:
            case HBASE_TABLE:
                return false;
            case DERBY_SYS_TABLE:
            case USER_TABLE:
                return true;
            default:
                throw new RuntimeException("Unknow table type " + type);
        }
    }
}
