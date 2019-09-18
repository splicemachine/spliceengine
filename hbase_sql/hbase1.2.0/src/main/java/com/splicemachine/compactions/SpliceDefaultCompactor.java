/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.hbase.SpliceCompactionUtils;
import com.splicemachine.olap.DistributedCompaction;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.server.CompactionContext;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.fs.HFileSystem;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFileContext;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionThroughputController;
import org.apache.hadoop.hbase.regionserver.compactions.NoLimitCompactionThroughputController;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;

/**
 *
 * Splicemachine Compactor with added code for locality.
 *
 */
public class SpliceDefaultCompactor extends SpliceDefaultCompactorBase {
    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        super(conf, store);
    }

    public SpliceDefaultCompactor(final Configuration conf, final Store store, long smallestReadPoint) {
        super(conf, store, smallestReadPoint);
    }

    @Override
    public List<Path> compact(CompactionRequest request, CompactionThroughputController throughputController, User user) throws IOException {
        if(!allowSpark || store.getRegionInfo().isSystemTable())
            return super.compact(request, throughputController,user);
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

        ScanType scanType =
                request.isRetainDeleteMarkers()
                        ? ScanType.COMPACT_RETAIN_DELETES
                        : ScanType.COMPACT_DROP_DELETES;
        // trigger MemstoreAwareObserver
        postCreateCoprocScanner(request, scanType, null,user);
        if (hostName == null)
            hostName = RSRpcServices.getHostname(conf,false);

        SConfiguration config = HConfiguration.getConfiguration();
        DistributedCompaction jobRequest=new DistributedCompaction(
                getCompactionFunction(request.isMajor(),getFavoredNodes()),
                files,
                getJobDetails(request),
                getJobGroup(request,hostName),
                getJobDescription(request),
                getPoolName(),
                getScope(request),
                hostName,
                config.getOlapCompactionMaximumWait());
        CompactionResult result = null;
        Future<CompactionResult> futureResult = EngineDriver.driver().getOlapClient().submit(jobRequest, getCompactionQueue());
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
                    return super.compact(request, throughputController, user);
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


        SpliceCompactionRequest scr = (SpliceCompactionRequest) request;
        scr.preStorefilesRename();

        List<Path> paths = new ArrayList<>();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }
        return paths;
    }

    public List<Path> sparkCompact(CompactionRequest request, CompactionContext context, InetSocketAddress[] favoredNodes) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "sparkCompact(): CompactionRequest=%s", request);

        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);

        // Find the smallest read point across all the Scanners.
        long smallestReadPoint = getSmallestReadPoint();

        List<StoreFileScanner> scanners;
        Collection<StoreFile> readersToClose;
        // Tell HDFS it can drop data out of the caches after reading them, we are compacting on Spark and won't need
        // that data anytime soon
        final boolean dropBehind = true;
        if (this.conf.getBoolean("hbase.regionserver.compaction.private.readers", false)) {
            // clone all StoreFiles, so we'll do the compaction on a independent copy of StoreFiles,
            // HFileFiles, and their readers
            readersToClose =new ArrayList<>(request.getFiles().size());
            for (StoreFile f : request.getFiles()) {
                readersToClose.add(new StoreFile(f));
            }
            scanners = createFileScanners(readersToClose, smallestReadPoint, dropBehind);
        } else {
            readersToClose = Collections.emptyList();
            scanners = createFileScanners(request.getFiles(), smallestReadPoint, dropBehind);
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
                    double resolutionShare = HConfiguration.getConfiguration().getOlapCompactionResolutionShare();
                    int bufferSize = HConfiguration.getConfiguration().getOlapCompactionResolutionBufferSize();
                    boolean blocking = HConfiguration.getConfiguration().getOlapCompactionBlocking();
                    SICompactionState state = new SICompactionState(driver.getTxnSupplier(),
                            driver.getConfiguration().getActiveTransactionCacheSize(), context, blocking ? driver.getExecutorService() : driver.getRejectingExecutorService());
                    boolean purgeDeletedRows = request.isMajor() && SpliceCompactionUtils.shouldPurge(store);

                    SICompactionScanner siScanner = new SICompactionScanner(state, scanner, purgeDeletedRows, resolutionShare, bufferSize, context);
                    siScanner.start();
                    scanner = siScanner;
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

                writer = createTmpWriter(fd, dropBehind,favoredNodes);
                boolean finished = performCompaction(fd, scanner,  writer, smallestReadPoint, cleanSeqId,
                        new NoLimitCompactionThroughputController(), request.isMajor());
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

    protected boolean performCompaction(FileDetails fd, InternalScanner scanner, CellSink writer,
                                        long smallestReadPoint, boolean cleanSeqId,
                                        CompactionThroughputController throughputController, boolean major) throws IOException {
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
        ScannerContext scannerContext =
                ScannerContext.newBuilder().setBatchLimit(compactionKVMax).build();
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

    /**
     * Creates a writer for a new file in a temporary directory.  This is pulled forward from DefaultCompactor
     * to handle some scoping issues.
     *
     * @param fd The file details.
     * @return Writer for a new StoreFile in the tmp dir.
     * @throws IOException
     */
    protected StoreFile.Writer createTmpWriter(FileDetails fd, boolean shouldDropBehind, InetSocketAddress[] favoredNodes)
            throws IOException {

        // When all MVCC readpoints are 0, don't write them.
        // See HBASE-8166, HBASE-12600, and HBASE-13389.

        return createWriterInTmp(fd.maxKeyCount, this.compactionCompression,
                /* isCompaction = */ true,
                /* includeMVCCReadpoint = */ fd.maxMVCCReadpoint > 0,
                /* includesTags = */ fd.maxTagsLength > 0,
                /* shouldDropBehind = */ shouldDropBehind,
                favoredNodes);
    }

    /**
     *
     * createWriterInTmp borrowed from DefaultCompactor to fix scope issues.
     *
     * @param maxKeyCount
     * @param compression
     * @param isCompaction
     * @param includeMVCCReadpoint
     * @param includesTag
     * @param shouldDropBehind
     * @param favoredNodes
     * @return
     * @throws IOException
     */
    public StoreFile.Writer createWriterInTmp(long maxKeyCount, Compression.Algorithm compression,
                                              boolean isCompaction, boolean includeMVCCReadpoint, boolean includesTag,
                                              boolean shouldDropBehind, InetSocketAddress[] favoredNodes)
            throws IOException {
        final CacheConfig writerCacheConf;
        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG,"createWriterInTmp with favoredNodes=%s",favoredNodes==null?"null": Arrays.toString(favoredNodes));

        }
        if (isCompaction) {
            // Don't cache data on write on compactions.
            writerCacheConf = new CacheConfig(store.getCacheConfig());
            writerCacheConf.setCacheDataOnWrite(false);
        } else {
            writerCacheConf = store.getCacheConfig();
        }
        // Required for Hbase Writer to pass on Favored Nodes
        HFileSystem wrappedFileSystem = new HFileSystem(store.getFileSystem());

        HFileContext hFileContext = createFileContext(compression, includeMVCCReadpoint, includesTag,
                getCryptoContext());
        StoreFile.Writer w = new StoreFile.WriterBuilder(conf, writerCacheConf,
                wrappedFileSystem)
                .withFilePath( ((HStore)store).getRegionFileSystem().createTempName())
                .withComparator(store.getComparator())
                .withBloomType(store.getFamily().getBloomFilterType())
                .withMaxKeyCount(maxKeyCount)
                .withFavoredNodes(favoredNodes)
                .withFileContext(hFileContext)
                .withShouldDropCacheBehind(shouldDropBehind)
                .build();
        return w;
    }

    public List<StoreFileScanner> createFileScanners(
            final Collection<StoreFile> filesToCompact,
            long smallestReadPoint) throws IOException {
        return StoreFileScanner.getScannersForStoreFiles(filesToCompact,
        /* cache blocks = */ false,
        /* use pread = */ false,
        /* is compaction */ true,
        /* use Drop Behind */ false,
                smallestReadPoint);
    }
}
