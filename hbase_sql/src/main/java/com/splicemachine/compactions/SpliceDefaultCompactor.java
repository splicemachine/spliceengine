package com.splicemachine.compactions;

import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
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

public class SpliceDefaultCompactor extends DefaultCompactor {
    private static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);
    private long smallestReadPoint;

    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        super(conf, store);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"init with store=%s",store.toString());
    }

    public SpliceDefaultCompactor(final Configuration conf, final Store store, long smallestReadPoint) {
        this(conf,store);
        this.smallestReadPoint = smallestReadPoint;
    }
    @Override
    public List<Path> compact(CompactionRequest request) throws IOException {
        smallestReadPoint = store.getSmallestReadPoint();
        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);
        List<String> files = new ArrayList<>();
        for (StoreFile sf : request.getFiles()) {
            files.add(sf.getPath().toString());
        }
        List<String> sPaths = SpliceSpark.getContext().parallelize(files, 1).mapPartitions(
                new SparkCompactionFunction(smallestReadPoint,store.getTableName().getNamespace(),store.getTableName().getQualifier(),store.getRegionInfo(),store.getFamily().getName())).collect();
        System.out.println("Paths Returned -> " + sPaths);
        this.progress.complete();

        List<Path> paths = new ArrayList();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }
        return paths;
    }

    public List<Path> sparkCompact(CompactionRequest request) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"compact compactionRequest=%s",request);

        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);

        // Find the smallest read point across all the Scanners.
        long smallestReadPoint = getSmallestReadPoint();

        List<StoreFileScanner> scanners;
        Collection<StoreFile> readersToClose;
        if (this.conf.getBoolean("hbase.regionserver.compaction.private.readers", false)) {
            // clone all StoreFiles, so we'll do the compaction on a independent copy of StoreFiles,
            // HFileFiles, and their readers
            readersToClose = new ArrayList<StoreFile>(request.getFiles().size());
            for (StoreFile f : request.getFiles()) {
                readersToClose.add(new StoreFile(f));
            }
            scanners = createFileScanners(readersToClose, smallestReadPoint);
        } else {
            readersToClose = Collections.emptyList();
            scanners = createFileScanners(request.getFiles(), smallestReadPoint);
        }

        StoreFile.Writer writer = null;
        List<Path> newFiles = new ArrayList<Path>();
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
                scanner = postCreateCoprocScanner(request, scanType, scanner);
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
            // Throw the exception
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
//        if (LOG.isTraceEnabled())
//            SpliceLogUtils.trace(LOG,"getProgress");
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
        List<Cell> cells = new ArrayList<Cell>();
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
}
