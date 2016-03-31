package org.apache.hadoop.hbase.regionserver;

import org.apache.hadoop.hbase.client.IsolationLevel;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Splice Machine consistency control implementation class, alternative
 * to the default HBase class {@link MultiVersionConsistencyControl}.
 */
public class SIMultiVersionConsistencyControl implements ConsistencyControl {
    private static final long NO_WRITE_NUMBER = 0;
    private AtomicLong memstoreRead = new AtomicLong(0);
    private final ConcurrentHashMap<RegionScanner, Long> scannerReadPoints =
        new ConcurrentHashMap<RegionScanner, Long>();
    /**
     * Default constructor. Initializes the memstoreRead/Write points to 0.
     */
    public SIMultiVersionConsistencyControl() {
        super();
    }

    public void initialize(long startPoint) {
        memstoreRead.set(startPoint);
    }

    public WriteEntry beginMemstoreInsert() {
        return beginMemstoreInsertWithSeqNum(NO_WRITE_NUMBER);
    }

    public WriteEntry beginMemstoreInsertWithSeqNum(long curSeqNum) {
        return new WriteEntry(curSeqNum);
    }

    public void completeMemstoreInsertWithSeqNum(WriteEntry e, SequenceId seqId)
        throws IOException {
        if(e == null) return;
        if (seqId != null) {
            e.setWriteNumber(seqId.getSequenceId());
        } else {
            // set the value to NO_WRITE_NUMBER in order NOT to advance memstore readpoint inside
            // function beginMemstoreInsertWithSeqNum in case of failures
            e.setWriteNumber(NO_WRITE_NUMBER);
        }
        waitForPreviousTransactionsComplete(e);
    }

    public void completeMemstoreInsert(WriteEntry e) {
        waitForPreviousTransactionsComplete(e);
    }

    public boolean advanceMemstore(WriteEntry e) {
        long nextReadValue = -1;
        e.markCompleted();
        while (true) {
            long currentRead = memstoreRead.get();
            if (e.getWriteNumber() <= currentRead) // already large
                return true;
            if (memstoreRead.compareAndSet(currentRead,e.getWriteNumber()))
                return true;
        }
    }

    public void waitForPreviousTransactionsComplete() {
        WriteEntry w = beginMemstoreInsert();
        waitForPreviousTransactionsComplete(w);
    }

    public void waitForPreviousTransactionsComplete(WriteEntry waitedEntry) {
        advanceMemstore(waitedEntry);
    }

    public long memstoreReadPoint() {
        return memstoreRead.get();
    }

    @Override
    public void advanceMemstoreReadPointIfNeeded(long seqNumber) {
        while (true) {
            long currentReadpt = memstoreRead.get();
            if (currentReadpt < seqNumber) {
                if (memstoreRead.compareAndSet(currentReadpt, seqNumber)) ;
                    return;
            } else
                return;
        }
    }

    @Override
    public long getReadpoint(IsolationLevel isolationLevel) {
        if (isolationLevel == IsolationLevel.READ_UNCOMMITTED) {
            // This scan can read even uncommitted transactions
            return Long.MAX_VALUE;
        }
        return memstoreReadPoint();
    }

    @Override
    public long addScan(RegionScanner regionScanner, Scan scan) {
        long readPt = getReadpoint(scan.getIsolationLevel());
        scannerReadPoints.put(regionScanner, readPt);
        return readPt;
    }

    @Override
    public void removeScan(RegionScanner regionScanner) {
        scannerReadPoints.remove(regionScanner);
    }

    @Override
    public long getSmallestReadPoint() {
        long minimumReadPt = memstoreReadPoint();
        for (Long readPoint: this.scannerReadPoints.values()) {
            if (readPoint < minimumReadPt) {
                minimumReadPt = readPoint;
            }
        }
        return minimumReadPt;
    }
}
