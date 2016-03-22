package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.compaction.SparkCompactionFunction;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkFlatMapFunction;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.commons.io.FileUtils;
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
import org.apache.spark.api.java.JavaRDD;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

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
        if(!allowSpark)
            return super.compact(request);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "compact(): request=%s", request);

        smallestReadPoint = store.getSmallestReadPoint();
        FileDetails fd = getFileDetails(request.getFiles(), request.isAllFiles());
        this.progress = new CompactionProgress(fd.maxKeyCount);
        List<String> files = new ArrayList<>();
        for (StoreFile sf : request.getFiles()) {
            files.add(sf.getPath().toString());
        }

        CompactionResult result = EngineDriver.driver().getOlapClient().submitOlapJob(
                new OlapCompaction(
                        getCompactionFunction(),
                        files,
                        getJobDetails(request),
                        getJobGroup(request),
                        getJobDescription(request),
                        getPoolName(),
                        getScope(request)));
        List<String> sPaths = result.getPaths();

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "Paths Returned: %s", sPaths);

        this.progress.complete();

        List<Path> paths = new ArrayList();
        for (String spath : sPaths) {
            paths.add(new Path(spath));
        }
        return paths;
    }

    protected SparkCompactionFunction getCompactionFunction() {
        return new SparkCompactionFunction(
            smallestReadPoint,
            store.getTableName().getNamespace(),
            store.getTableName().getQualifier(),
            store.getRegionInfo(),
            store.getFamily().getName());
    }

    protected String getScope(CompactionRequest request) {
        return String.format("%s Compaction: %s",
            getMajorMinorLabel(request),
            getTableInfoLabel(", "));
    }

    protected String getJobDescription(CompactionRequest request) {
        int size = request.getFiles().size();
        return String.format("%s Compaction: %s, %d %s",
            getMajorMinorLabel(request),
            getTableInfoLabel(", "),
            size,
            (size > 1 ? "Files" : "File"));
    }

    protected String getMajorMinorLabel(CompactionRequest request) {
        return request.isMajor() ? "Major" : "Minor";
    }

    protected String getTableInfoLabel(String delim) {
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

    protected String getJobGroup(CompactionRequest request) {
        // TODO: we can probably do better than this for unique job group
        return Long.toString(request.getSelectionTime());
    }

    private static String delim = ",\n";
    private String jobDetails = null;
    protected String getJobDetails(CompactionRequest request) {
        if (jobDetails == null) {
            StringBuilder sb = new StringBuilder();
            sb.append(getTableInfoLabel(delim));
            sb.append(delim).append(String.format("Region Name=%s", this.store.getRegionInfo().getRegionNameAsString()));
            sb.append(delim).append(String.format("Region Id=%d", this.store.getRegionInfo().getRegionId()));
            sb.append(delim).append(String.format("File Count=%d", request.getFiles().size()));
            sb.append(delim).append(String.format("Total File Size=%s", FileUtils.byteCountToDisplaySize(request.getSize())));
            sb.append(delim).append(String.format("Type=%s", getMajorMinorLabel(request)));
            jobDetails = sb.toString();
        }
        return jobDetails;
    }

    protected String getPoolName() {
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
