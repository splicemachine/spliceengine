package com.splicemachine.stats;

/**
 * Represents information about a specific table.
 *
 * Note that there is a separate TableStatistics entity for an index
 * versus for a base table.
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface TableStatistics {

    /**
     * @return the estimated row count for the table.
     */
    long rowCount();

    /**
     * @return the average size of a single row in the table (in bytes)
     */
    int averageRowSize();

    /**
     * @return the number of regions in the table.
     */
    int regionCount();

    /**
     * estimate the cost to scan the entire table--that is, generate an estimate
     * of how long it would take to scan every row in the table.
     *
     * @param parallel if {@code true}, then estimate the length of time taken
     *                 to scan the table in parallel. If {@code false}, then estimate
     *                 the time to scan the entire table sequentially.
     * @return an estimated cost (in <em>microseconds</em>) to scan the entire table
     *         either in parallel or sequentially
     */
    double estimateScanCost(boolean parallel);

    /**
     * Estimate the cost to scan only {@code numRows} rows from the table.
     *
     * @param numRows the number of rows to scan.
     * @param parallel if {@code true}, if {@code true}, then estimate the length of time taken
     *                 to scan the table in parallel. If {@code false}, then estimate
     *                 the time to scan the entire table sequentially.
     * @return an estimated cost (in <em>microseconds</em>) to scan {@code numRows} from the entire table
     *         either in parallel or sequentially.
     */
    double estimateScanCost(long numRows, boolean parallel);

    /**
     * Get the Column statistics for the column specified.
     *
     * @param columnNum the index of the column in the table. This value is
     *                  indexed from 0.
     * @return the ColumnStatistics for the specified column.
     * @throws java.lang.IllegalArgumentException if {@code columnNum} is larger than
     * the number of columns in the table.
     */
    ColumnStatistics getColumnStatistics(int columnNum);

    /**
     * @return the statistics for the HBase key.
     */
    ColumnStatistics getKeyStatistics();
}
