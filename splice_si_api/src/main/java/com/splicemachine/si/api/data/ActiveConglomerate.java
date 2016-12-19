package com.splicemachine.si.api.data;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 *
 *
 *
 */
public interface ActiveConglomerate {
    /**
     *
     * Transaction ID 1
     *
     * @return
     */
    long getTransactionID1();

    /**
     *
     * Version Number of Updates, required due to
     * read committed nature of select for update functionality.
     *
     * @return
     */
    long getVersion();
    /**
     *
     * Tombstone Marker For Deletes
     *
     */
    boolean hasTombstone();

    /**
     *
     * Transaction ID representing the parent timestamp of a hierarchical
     * transaction or the increment plus node id of a collapsible transaction.
     *
     * @return
     */
    long getTransactionID2();


    /**
     *
     * The effective commit timestamp of the data, -1 if rolledback
     *
     * @return
     */
    long getEffectiveTimestamp();


    /**
     *
     * Number of Columns
     *
     * @return
     */
    int numberOfColumns();


    /**
     *
     * Number of Columns
     *
     * @return
     */
    UnsafeRow getData();

}
