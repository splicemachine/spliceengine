package com.splicemachine.derby.iapi.storage;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

public interface ScanBoundary{
    /**
     * Constructs a scan from the given start and finish row keys.
     *
     * This allows the Boundary to construct Scans with appropriate ObserverInstructions,
     * caching, batching, and so on.
     *
     * @param start the start key of the scan
     * @param finish the end key of the scan
     * @return a constructed Scan representing the range given by {@code start} and {@code finish}
     */
    Scan buildScan(byte[] start, byte[] finish);

    /**
     * Get the beginning of the key range that this row belongs to.
     *
     * @param result the row of interest
     * @return the start key for the row.
     */
    byte[] getStartKey(Result result);

    /**
     * Get the end of the key range that this row key belongs to (and/or the start of the
     * next row key).
     *
     * @param result the row of interest
     * @return the end key for the row (and/or) the start key of the key range immediately following.
     */
    byte[] getStopKey(Result result);

    /**
     * Determines if the RowProvider should perform an additional <em>lookbehind</em>
     * scan.
     *
     * This is different from shouldStartLate(), in that if this method returns true, then
     * getLookBehindStartKey(byte[]) will be called; thus, an <em>additional</em> remote scan
     * will be performed to pull data which is located on a different region, but which belongs
     * with this region's scan data.
     *
     * @param firstRowInRegion the first row in this local region.
     * @return true if the row provider should perform an additional remote scan for data
     * that occurs <em>before</em> the start of this local region.
     */
    boolean shouldLookBehind(byte[] firstRowInRegion);

    /**
     * Determines if the scanner should skip past all row keys with this key prefix locally.
     *
     * This is in opposition to shouldLookBehind()--That is, if shouldLookBehind() returns true,
     * then shouldStartLate cannot be true.
     *
     * @param firstRowInRegion the first row in this local region
     * @return true if the row provider should skip all local keys in this key prefix range.
     */
    boolean shouldStartLate(byte[] firstRowInRegion);

    /**
     * Determines if the scanner should stop early.
     *
     * This is in opposition to shouldLookAhead()--That is, if shouldLookAhead() returns true,
     * then shouldStopEarly() cannot return true.
     *
     * @param firstRowInNextRegion the first row in the next region
     * @return true if the row provider should stop at the beginning of this key prefix range.
     */
    boolean shouldStopEarly(byte[] firstRowInNextRegion);

    /**
     * Determines if the scanner should perform a lookahead.
     *
     * This is in opposition to shouldStopEarly()--That is, if shouldStopEarly() returns true,
     * then shouldLookAhead() cannot return true.
     *
     * @param firstRowInNextRegion the first row in the next region
     * @return true if the row provider should look ahead to finish this key prefix range.
     */
    boolean shouldLookAhead(byte[] firstRowInNextRegion);
}

