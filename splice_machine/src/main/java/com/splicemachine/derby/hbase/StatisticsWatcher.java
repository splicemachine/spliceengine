package com.splicemachine.derby.hbase;

/**
 * Interface for watching for Statistics Updates. Implementations may choose
 * to do things when row counts exceed a certain threshold (e.g. automatically trigger
 * a statistics re-collection, or automatically mark statistics stale).
 *
 * @author Scott Fines
 *         Date: 4/7/15
 */
public interface StatisticsWatcher{

    /**
     * Indicate that {@code rowCount} rows were successfully written
     * to the underlying conglomerate.
     *
     * This method is expected to be called for every batch of records, which means
     * it needs to be very low-impact--any expensive operations should be performed
     * asynchronously.
     *
     * @param rowCount the number of rows which were successfully written to the
     *                 underlying conglomerate
     */
    void rowsWritten(long rowCount);
}
