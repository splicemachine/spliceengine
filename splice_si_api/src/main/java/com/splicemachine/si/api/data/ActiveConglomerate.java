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
    long getTransactionId1();

    /**
     *
     * Transaction ID 1
     *
     * @return
     */
    void setTransactionId1(long transactionId1);

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
     * Version Number of Updates, required due to
     * read committed nature of select for update functionality.
     *
     * @return
     */
    void setVersion(long version);

    /**
     *
     * Tombstone Marker For Deletes
     *
     */
    boolean hasTombstone();

    /**
     *
     * Tombstone Marker For Deletes
     *
     */
    void setTombstone(boolean hasTombstone);

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
     * Transaction ID representing the parent timestamp of a hierarchical
     * transaction or the increment plus node id of a collapsible transaction.
     *
     * @return
     */
    void setTransactionID2(long transactionID2);

    /**
     *
     * The effective commit timestamp of the data, -1 if rolledback
     *
     * @return
     */
    long getEffectiveTimestamp();

    /**
     *
     * The effective commit timestamp of the data, -1 if rolledback
     *
     * @return
     */
    void setEffectiveTimestamp(long effectiveTimestamp);


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
    void setNumberOfColumns(int numberOfColumns);


    /**
     *
     * Set Actual Data
     *
     * @return
     */
    UnsafeRow getData();

    /**
     *
     * Set Actual Data
     *
     * @return
     */
    void setData(UnsafeRow data);



}
