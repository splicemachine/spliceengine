package com.splicemachine.compactions;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.*;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionProgress;
import org.apache.hadoop.hbase.regionserver.compactions.CompactionRequest;
import org.apache.hadoop.hbase.regionserver.compactions.DefaultCompactor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * Created by jleach on 11/5/15.
 */
public class SpliceDefaultCompactor extends DefaultCompactor {
    private static final Logger LOG = Logger.getLogger(SpliceDefaultCompactor.class);

    public SpliceDefaultCompactor(final Configuration conf, final Store store) {
        super(conf, store);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"init with store=%s",store.toString());
    }

    @Override
    public List<Path> compact(CompactionRequest request) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"compact compactionRequest=%s",request);
        return super.compact(request);
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
        return super.getSmallestReadPoint();
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
        return super.performCompaction(fd, scanner, writer, smallestReadPoint, cleanSeqId, major);
    }

    @Override
    protected InternalScanner createScanner(Store store, List<StoreFileScanner> scanners, ScanType scanType, long smallestReadPoint, long earliestPutTs) throws IOException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"createScanner");
        return super.createScanner(store, scanners, scanType, smallestReadPoint, earliestPutTs);
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
