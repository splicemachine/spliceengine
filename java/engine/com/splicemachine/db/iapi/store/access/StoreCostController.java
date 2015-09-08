/*

   Derby - Class com.splicemachine.db.iapi.store.access.StoreCostController

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package com.splicemachine.db.iapi.store.access;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import java.util.BitSet;

/**
 * The StoreCostController interface provides methods that an access client
 * (most likely the system optimizer) can use to get store's estimated cost of
 * various operations on the conglomerate the StoreCostController was opened
 * for.
 * <p/>
 * It is likely that the implementation of StoreCostController will open
 * the conglomerate and will leave the conglomerate open until the
 * StoreCostController is closed.  This represents a significant amount of
 * work, so the caller if possible should attempt to open the StoreCostController
 * once per unit of work and rather than close and reopen the controller.  For
 * instance if the optimizer needs to cost 2 different scans against a single
 * conglomerate, it should use one instance of the StoreCostController.
 * <p/>
 * The locking behavior of the implementation of a StoreCostController is
 * undefined, it may or may not get locks on the underlying conglomerate.  It
 * may or may not hold locks until end of transaction.
 * An optimal implementation will not get any locks on the underlying
 * conglomerate, thus allowing concurrent access to the table by a executing
 * query while another query is optimizing.
 *
 * @see TransactionController#openStoreCost
 * @see RowCountable
 */

public interface StoreCostController extends RowCountable{

    /**
     * Close the controller.
     * <p/>
     * Close the open controller.  This method always succeeds, and never
     * throws any exceptions. Callers must not use the StoreCostController
     * Cost controller after closing it; they are strongly advised to clear
     * out the scan controller reference after closing.
     * <p/>
     *
     * @throws StandardException Standard exception policy.
     */
    void close() throws StandardException;

    /**
     * Return the cost of exact key lookup.
     * <p/>
     * Return the estimated cost of calling ScanController.fetch()
     * on the current conglomerate, with start and stop positions set such
     * that an exact match is expected.
     * <p/>
     * This call returns the cost of a fetchNext() performed on a scan which
     * has been positioned with a start position which specifies exact match
     * on all keys in the row.
     * <p/>
     * Example:
     * <p/>
     * In the case of a btree this call can be used to determine the cost of
     * doing an exact probe into btree, giving all key columns.  This cost
     * can be used if the client knows it will be doing an exact key probe
     * but does not have the key's at optimize time to use to make a call to
     * getScanCost()
     * <p/>
     *
     * @param validColumns A description of which columns to return from
     *                     row on the page into "templateRow."  templateRow,
     *                     and validColumns work together to
     *                     describe the row to be returned by the fetch -
     *                     see RowUtil for description of how these three
     *                     parameters work together to describe a fetched
     *                     "row".
     * @param access_type  Describe the type of access the query will be
     *                     performing to the ScanController.
     *                     <p/>
     *                     STORECOST_CLUSTERED - The location of one scan
     *                     is likely clustered "close" to the previous
     *                     scan.  For instance if the query plan were
     *                     to used repeated "reopenScan()'s" to probe
     *                     for the next key in an index, then this flag
     *                     should be be specified.  If this flag is not
     *                     set then each scan will be costed independant
     *                     of any other predicted scan access.
     * @return The cost of the fetch.
     * @throws StandardException Standard exception policy.
     * @see RowUtil
     */
    void getFetchFromFullKeyCost(BitSet validColumns,
                                 int access_type,
                                 CostEstimate cost) throws StandardException;

    /**
     * Return an "empty" row location object of the correct type.
     * <p/>
     *
     * @return The empty Rowlocation.
     * @throws StandardException Standard exception policy.
     */
    RowLocation newRowLocationTemplate() throws StandardException;

    /**
     * Get the selectivity fraction for the specified range and the specified column.
     * <p/>
     * The <em>selectivity fraction</em> is a number in the range {@code [0,1]} that indicates
     * the percentage of rows in the data set which <em>matches</em> the range of data.
     * <p/>
     * If no statistics exist, then this should be 1.0d
     *
     * @param columnNumber the id of the column to perform estimate for (indexed from 1)
     * @param start        the value for the start of the range, or {@code null} if no stop is estimated
     * @param includeStart whether to include the start value in the estimate
     * @param stop         the value for the stop of the range, or {@code null} if no stop is estimated
     * @param includeStop  whether to include the stop value in the estimate
     * @return an estimate of the selectivity fraction
     */
    double getSelectivity(int columnNumber,
                          DataValueDescriptor start,
                          boolean includeStart,
                          DataValueDescriptor stop,boolean includeStop);

    /**
     * @return the total number of rows in the store (including null and non-null)
     */
    double rowCount();

    /**
     * @return the total number of non-null rows in the store for the specified column
     * @param columnNumber the column of interest (indexed from 1);
     */
    double nonNullCount(int columnNumber);

    /**
     * Get the selectivity fraction for {@code null} entries for the specified column.
     *
     * @param columnNumber the id of the column to estimate (indexed from 1)
     * @return an estimate of the percentage of rows in the data set which are null.
     */
    double nullSelectivity(int columnNumber);

    /**
     *
     * Retrieve the cardinality for the specified column.  If not available, returns 0.
     *
     * @param columnNumber the id of the column to estimate (indexed from 1)
     * @return an estimate of the number of distinct entries (cardinality).
     */
    long cardinality(int columnNumber);

    /**
     *
     * Get Average Row Width of the Conglomerate
     *
     * @return
     */
    long getConglomerateAvgRowWidth();

    /**
     *
     * Get Average Row Width of the Base Table even if the conglomerate is an index.  This is critical for
     * normalizing data between indexes and base tables.
     *
     * @return
     */
    long getBaseTableAvgRowWidth();

    /**
     *
     * Currently a static factor representing the cost of scanning one row of data.
     *
     * @return
     */
    double getLocalLatency();

    /**
     *
     * Currently a static factor representing the cost of doing a remote get on one row of data.
     *
     * @return
     */
    double getRemoteLatency();

    /**
     *
     * Number of partitions involved.  TODO: JL Need a better way of determining number of partitions involved in a query.
     *
     * @return
     */
    int getNumPartitions();

    /**
     *
     * Column Size factor of the current conglomerate.  This represents the ratio of data being returned.
     *
     * @param validColumns
     * @return
     */
    double conglomerateColumnSizeFactor(BitSet validColumns);

    /**
     * Column Size factor for the base table regardless of conglomerate being evaluated.  This represents the ratio
     * of the data being returned.
     *
     * @param validColumns
     * @return
     */
    double baseTableColumnSizeFactor(BitSet validColumns);

    /**
     * @return the total number of rows in the base conglomerate (including null and non-null)
     */
    double baseRowCount();

}
