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

package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.constants.EnvUtils;
import com.splicemachine.derby.iapi.sql.olap.OlapClient;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.olap.DistributedCompaction;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.data.hbase.coprocessor.TableType;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class SpliceDefaultCompactor extends DefaultCompactor {
    private static final boolean allowSpark = true;
    private static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);
    private long smallestReadPoint;
    private String conglomId = null;
    private String tableDisplayName = null;
    private String indexDisplayName = null;

    private static final String TABLE_DISPLAY_NAME_ATTR = SIConstants.TABLE_DISPLAY_NAME_ATTR;
    private static final String INDEX_DISPLAY_NAME_ATTR = SIConstants.INDEX_DISPLAY_NAME_ATTR;

    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        super(conf, store);
        
        conglomId = this.store.getTableName().getQualifierAsString();
        tableDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(TABLE_DISPLAY_NAME_ATTR);
        indexDisplayName = ((HStore)this.store).getHRegion().getTableDesc().getValue(INDEX_DISPLAY_NAME_ATTR);

        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "Initializing compactor: region=%s", ((HStore)this.store).getHRegion());
        }
    }

    public SpliceDefaultCompactor(final Configuration conf, final Store store, long smallestReadPoint) {
        this(conf, store);
        this.smallestReadPoint = smallestReadPoint;
    }

    @Override
    public List<Path> compact(CompactionRequest request) throws IOException {
        if(!allowSpark || store.getRegionInfo().isSystemTable())
            return super.compact(request);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "compact(): request=%s", request);

        assert request instanceof SpliceCompactionRequest;

        smallestReadPoint = store.getSmallestReadPoint();
        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);
        List<String> files = new ArrayList<>();
        for (StoreFile sf : request.getFiles()) {
            files.add(sf.getPath().toString());
        }
        String regionLocation = getRegionLocation(store);
        SConfiguration config = HConfiguration.getConfiguration();
        DistributedCompaction jobRequest=new DistributedCompaction(
                getCompactionFunction(),
                files,
                getJobDetails(request),
                getJobGroup(request,regionLocation),
                getJobDescription(request),
                getPoolName(),
                getScope(request),
                regionLocation,
                config.getOlapCompactionMaximumWait());
        CompactionResult result = null;
        OlapClient olapClient = getOlapClient();
        Future<CompactionResult> futureResult = olapClient.submit(jobRequest);
        while(result == null) {
            try {
                result = futureResult.get(config.getOlapClientTickTime(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                //we were interrupted processing, so we're shutting down. Nothing to be done, just die gracefully
                Thread.currentThread().interrupt();
                throw new IOException(e);
            } catch (ExecutionException e) {
                if (e.getCause() instanceof RejectedExecutionException) {
                    LOG.warn("Spark compaction execution rejected, falling back to RegionServer execution", e.getCause());
                    return super.compact(request);
                }
                throw Exceptions.rawIOException(e.getCause());
            } catch (TimeoutException e) {
                // check region write status
                if (!store.areWritesEnabled()) {
                    futureResult.cancel(true);
                    progress.cancel();
                    // TODO should we cleanup files written by Spark?
                    throw new IOException("Region has been closed, compaction aborted");
                }
            }
        }

        List<String> sPaths = result.getPaths();

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Paths Returned: %s", sPaths);

        this.progress.complete();

        ScanType scanType = request.isRetainDeleteMarkers() ? ScanType.COMPACT_RETAIN_DELETES
                : ScanType.COMPACT_DROP_DELETES;
        // trigger MemstoreAwareObserver
        postCreateCoprocScanner(request, scanType, null);

        SpliceCompactionRequest scr = (SpliceCompactionRequest) request;
        scr.preStorefilesRename();

        List<Path> paths = new ArrayList<>();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }
        return paths;
    }

    private OlapClient getOlapClient() throws IOException {
        EngineDriver driver = null;
        OlapClient olapClient = null;
        // Give initialization 5 seconds to complete before aborting.
        // This is to handle compaction jobs running right away while
        // server restart is in progress.
        for (int i = 0; i < 5; i++) {
            try {
                driver = EngineDriver.driver();
                if (driver != null)
                    olapClient = driver.getOlapClient();
                if (olapClient != null)
                    return olapClient;
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                throw new InterruptedIOException();
            }
        }
        throw new IOException("Splice compaction service not initialized yet. Compaction aborted.");
    }

    private SparkCompactionFunction getCompactionFunction() {
        return new SparkCompactionFunction(
            smallestReadPoint,
            store.getTableName().getNamespace(),
            store.getTableName().getQualifier(),
            store.getRegionInfo(),
            store.getFamily().getName());
    }

    private String getScope(CompactionRequest request) {
        return String.format("%s Compaction: %s",
            getMajorMinorLabel(request),
            getTableInfoLabel(", "));
    }

    private String getJobDescription(CompactionRequest request) {
        int size = request.getFiles().size();
        return String.format("%s Compaction: %s, %d %s",
            getMajorMinorLabel(request),
            getTableInfoLabel(", "),
            size,
            (size > 1 ? "Files" : "File"));
    }

    private String getMajorMinorLabel(CompactionRequest request) {
        return request.isMajor() ? "Major" : "Minor";
    }

    private String getTableInfoLabel(String delim) {
        StringBuilder sb = new StringBuilder();
        if (indexDisplayName != null) {
            sb.append(String.format("Index=%s", indexDisplayName));
            sb.append(delim);
        } else if (tableDisplayName != null) {
            sb.append(String.format("Table=%s", tableDisplayName));
            sb.append(delim);
        }
        sb.append(String.format("Conglomerate=%s", conglomId));
        return sb.toString();
    }

    private String getJobGroup(CompactionRequest request,String regionLocation) {
        return regionLocation+":"+Long.toString(request.getSelectionTime());
    }

    private static final String delim = ",\n";
    private String getJobDetails(CompactionRequest request) {
        return getTableInfoLabel(delim) +delim
            +String.format("Region Name=%s",this.store.getRegionInfo().getRegionNameAsString()) +delim
            +String.format("Region Id=%d",this.store.getRegionInfo().getRegionId()) +delim
            +String.format("File Count=%d",request.getFiles().size()) + (request.isAllFiles() ? " (all)" : "") +delim
            +String.format("Total File Size=%s",FileUtils.byteCountToDisplaySize(request.getSize())) +delim
            +String.format("Type=%s",getMajorMinorLabel(request));
    }

    private String getPoolName() {
        return "compaction";
    }

    public List<Path> sparkCompact(CompactionRequest request) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "sparkCompact(): CompactionRequest=%s", request);

        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);

        // Find the smallest read point across all the Scanners.
        long smallestReadPoint = getSmallestReadPoint();

        List<StoreFileScanner> scanners;
        Collection<StoreFile> readersToClose;
        if (this.conf.getBoolean("hbase.regionserver.compaction.private.readers", false)) {
            // clone all StoreFiles, so we'll do the compaction on a independent copy of StoreFiles,
            // HFileFiles, and their readers
            readersToClose =new ArrayList<>(request.getFiles().size());
            for (StoreFile f : request.getFiles()) {
                readersToClose.add(new StoreFile(f));
            }
            scanners = createFileScanners(readersToClose, smallestReadPoint);
        } else {
            readersToClose = Collections.emptyList();
            scanners = createFileScanners(request.getFiles(), smallestReadPoint);
        }

        StoreFile.Writer writer = null;
        List<Path> newFiles =new ArrayList<>();
        boolean cleanSeqId = false;
        IOException e = null;
        try {
            InternalScanner scanner = null;
            try {
                /* Include deletes, unless we are doing a compaction of all files */
                ScanType scanType = request.isRetainDeleteMarkers() ? ScanType.COMPACT_RETAIN_DELETES
                        : ScanType.COMPACT_DROP_DELETES;
                scanner = preCreateCoprocScanner(request, scanType, fd.earliestPutTs, scanners);
                if (scanner == null) {
                    scanner = createScanner(store, scanners, scanType, smallestReadPoint, fd.earliestPutTs);
                }
                if (needsSI(store.getTableName())) {
                    SIDriver driver=SIDriver.driver();
                    SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                            driver.getRollForward(),
                            driver.getConfiguration().getActiveTransactionCacheSize());
                    scanner = new SICompactionScanner(state,scanner);
                }
                if (scanner == null) {
                    // NULL scanner returned from coprocessor hooks means skip normal processing.
                    return newFiles;
                }
                // Create the writer even if no kv(Empty store file is also ok),
                // because we need record the max seq id for the store file, see HBASE-6059
                if(fd.minSeqIdToKeep > 0) {
                    smallestReadPoint = Math.min(fd.minSeqIdToKeep, smallestReadPoint);
                    cleanSeqId = true;
                }

                writer = createTmpWriter(fd, smallestReadPoint);
                boolean finished = performCompaction(fd, scanner, writer, smallestReadPoint, cleanSeqId,
                        request.isAllFiles());
                if (!finished) {
                    writer.close();
                    store.getFileSystem().delete(writer.getPath(), false);
                    writer = null;
                    throw new InterruptedIOException( "Aborting compaction of store " + store +
                            " in region " + store.getRegionInfo().getRegionNameAsString() +
                            " because it was interrupted.");
                }
            } finally {
                if (scanner != null) {
                    scanner.close();
                }
            }
        } catch (IOException ioe) {
            e = ioe;
            throw ioe;
        }
        finally {
            try {
                if (writer != null) {
                    if (e != null) {
                        writer.close();
                    } else {
                        writer.appendMetadata(fd.maxSeqId, request.isAllFiles());
                        writer.close();
                        newFiles.add(writer.getPath());
                    }
                }
            } finally {
                for (StoreFile f : readersToClose) {
                    try {
                        f.closeReader(true);
                    } catch (IOException ioe) {
                        LOG.warn("Exception closing " + f, ioe);
                    }
                }
            }
        }
        return newFiles;
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

    @Override
    protected StoreFile.Writer createTmpWriter(FileDetails fd, long smallestReadPoint) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createTempWriter");
        return super.createTmpWriter(fd, smallestReadPoint);
    }

    @Override
    public List<Path> compactForTesting(Collection<StoreFile> filesToCompact, boolean isMajor) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"compactForTesting");
        return super.compactForTesting(filesToCompact, isMajor);
    }

    @Override
    public CompactionProgress getProgress() {
        return super.getProgress();
    }

    @Override
    protected FileDetails getFileDetails(Collection<StoreFile> filesToCompact, boolean allFiles) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFileDetails");
        return super.getFileDetails(filesToCompact, allFiles);
    }

    @Override
    protected List<StoreFileScanner> createFileScanners(Collection<StoreFile> filesToCompact, long smallestReadPoint) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createFileScanners");
        return super.createFileScanners(filesToCompact, smallestReadPoint);
    }

    @Override
    protected long getSmallestReadPoint() {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getSmallestReadPoint");
        return this.smallestReadPoint;
    }

    @Override
    protected InternalScanner preCreateCoprocScanner(CompactionRequest request, ScanType scanType, long earliestPutTs, List<StoreFileScanner> scanners) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"preCreateCoprocScanner");
        return super.preCreateCoprocScanner(request, scanType, earliestPutTs, scanners);
    }

    @Override
    protected InternalScanner postCreateCoprocScanner(CompactionRequest request, ScanType scanType, InternalScanner scanner) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"postCreateCoprocScanner");
        return super.postCreateCoprocScanner(request, scanType, scanner);
    }

    @Override
    protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer, long smallestReadPoint, boolean cleanSeqId, boolean major) throws IOException {
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
        do {
            hasMore = scanner.next(cells, compactionKVMax);
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
                ++progress.currentCompactedKVs;
                progress.totalCompactedSize += len;
                if (LOG.isDebugEnabled()) {
                    bytesWrittenProgress += len;
                }
                // check periodically to see if a system stop is requested
                if (closeCheckInterval > 0) {
                    bytesWritten += len;
                    if (bytesWritten > closeCheckInterval) {
                        bytesWritten = 0;
//                        if (!store.areWritesEnabled()) {
//                            progress.cancel();
//                            return false;
//                        }
                    }
                }
            }
            // Log the progress of long running compactions every minute if
            // logging at DEBUG level
            if (LOG.isDebugEnabled()) {
                if ((now - lastMillis) >= 60 * 1000) {
                    LOG.debug("Compaction progress: " + progress + String.format(", rate=%.2f kB/sec",
                            (bytesWrittenProgress / 1024.0) / ((now - lastMillis) / 1000.0)));
                    lastMillis = now;
                    bytesWrittenProgress = 0;
                }
            }
            cells.clear();
        } while (hasMore);
        progress.complete();
        return true;
    }

    @Override
    protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        Scan scan = new Scan();
        scan.setMaxVersions(store.getFamily().getMaxVersions());
        return new StoreScanner(store, store.getScanInfo(), scan, scanners,
                scanType, smallestReadPoint, earliestPutTs);
    }

    @Override
    protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners, long smallestReadPoint, long earliestPutTs, byte[] dropDeletesFromRow, byte[] dropDeletesToRow) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        return super.createScanner(store, scanners, smallestReadPoint, earliestPutTs, dropDeletesFromRow, dropDeletesToRow);
    }

    @Override
    protected void resetSeqId(long smallestReadPoint, boolean cleanSeqId, KeyValue kv) {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"resetSeqId");
        super.resetSeqId(smallestReadPoint, cleanSeqId, kv);
    }

    @Override
    protected void appendMetadataAndCloseWriter(StoreFile.Writer writer, FileDetails fd, boolean isMajor) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"appendMetadataAndCloseWriter");
        super.appendMetadataAndCloseWriter(writer, fd, isMajor);
    }

    /**
     * Returns location for an HBase store
     * @param store the store whose location is needed
     * @return server host name where store is located
     * @throws IOException
     */
    @SuppressWarnings("unchecked")
    private String getRegionLocation(Store store) throws IOException {
        // Get start key for the store
        HRegionInfo regionInfo = store.getRegionInfo();
        byte[] startKey = regionInfo.getStartKey();

        // Get an instance of the table
        PartitionFactory tableFactory=SIDriver.driver().getTableFactory();
        Partition table = tableFactory.getTable(regionInfo.getTable());

        // Get region location using start key
        List<Partition> partitions = table.subPartitions(startKey, HConstants.EMPTY_END_ROW, true);
        if (partitions.isEmpty()) {
            throw new IOException("Couldn't find region location for " + regionInfo);
        }
        return partitions.get(0).owningServer().getHostname();
    }
}
