package com.splicemachine.stats.estimation;

import com.splicemachine.stats.estimation.TableStatistics;

/**
 * Represents costs which are specific to an Index table (such as
 * lookup costs).
 *
 * @author Scott Fines
 *         Date: 10/27/14
 */
public interface IndexStatistics extends TableStatistics {

    /**
     * @return the average cost to perform a single table lookup
     */
    long averageLookupCost();

    /**
     * Estimate the cost to perform {@code numRows} lookups (including
     * the cost to scan the index table <em>and</em> the cost of performing
     * a lookup on each row).
     *
     * This is very similar to estimating the cost of an IndexRowToBaseRow lookup.
     *
     * @param numRows the number of rows to lookup.
     * @param parallel if {@code true} then estimate as if {@code numRows} is performed
     *                 in parallel, otherwise estimate as if {@code numRows} is being
     *                 looked-up sequentially.
     * @return an estimated cost to perform an index row lookup on {@code numRows} rows.
     */
    double estimateLookupCost(long numRows,boolean parallel);

    /**
     * Estimate the cost to perform a lookup on every row in the table(including
     * the cost to scan the index table <em>and</em> the cost of performing
     * a lookup on each row).
     *
     * This is very similar to estimating the cost of an IndexRowToBaseRow lookup.
     *
     * @param parallel if {@code true} then estimate as if {@code numRows} is performed
     *                 in parallel, otherwise estimate as if {@code numRows} is being
     *                 looked-up sequentially.
     * @return an estimated cost to perform an index row lookup on the entire index
     */
    double estimateLookupCost(boolean parallel);
}
