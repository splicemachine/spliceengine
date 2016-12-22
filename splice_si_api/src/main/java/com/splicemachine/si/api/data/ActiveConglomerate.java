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
     * Txn ID 1
     *
     * @return
     */
    long getTransactionId1();

    /**
     *
     * Txn ID 1
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
    void setHasTombstone(boolean hasTombstone);

    /**
     *
     * Txn ID representing the parent timestamp of a hierarchical
     * transaction or the increment plus node id of a collapsible transaction.
     *
     * @return
     */
    long getTransactionId2();


    /**
     *
     * Txn ID representing the parent timestamp of a hierarchical
     * transaction or the increment plus node id of a collapsible transaction.
     *
     * @return
     */
    void setTransactionId2(long transactionId2);

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
