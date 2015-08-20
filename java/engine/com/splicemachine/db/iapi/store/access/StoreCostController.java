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
import java.util.List;

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
    // The folllowing constants should only be used by StoreCostController
    // implementors.

    // The base cost to fetch a cached page, and select a single
    // heap row by RowLocation, fetching 0 columns.
    double BASE_CACHED_ROW_FETCH_COST=0.17;

    // The base cost to page in a page from disk to cache, and select a single
    // heap row by RowLocation, fetching 0 columns.
    double BASE_UNCACHED_ROW_FETCH_COST=1.5;

    // The base cost to fetch a single row as part of group fetch scan with
    // 16 rows per group, fetching 0 columns.
    double BASE_GROUPSCAN_ROW_COST=0.12;

    // The base cost to fetch a single row as part of a nongroup fetch scan
    // fetching 0 columns.
    double BASE_NONGROUPSCAN_ROW_FETCH_COST=0.25;

    // The base cost to fetch a single row as part of a nongroup fetch scan
    // fetching 1 columns.
    double BASE_HASHSCAN_ROW_FETCH_COST=0.14;


    // This is an estimate of the per byte cost associated with fetching the 
    // row from the table, it just assumes the cost scales per byte which is 
    // probably not true, but a good first guess.  It is meant to be added
    // to the above costs - for instance the cost of fetching a 100 byte 
    // row from a page assumed to be in the cache is:
    //     BASE_CACHED_ROW_FETCH_COST + (100 * BASE_ROW_PER_BYTECOST)
    //
    // The estimate for this number is the cost of retrieving all cost from
    // a cached 100 byte row - the cost of getting 0 colums from cached row.
    double BASE_ROW_PER_BYTECOST=(0.56-0.16)/100;

    /**
     * Indicates that access to the page necessary to fulfill the fetch
     * request is likely to be a page "recently" used.  See
     * getFetchFromFullKeyCost() and getScanCost().
     */
    int STORECOST_CLUSTERED=0x01;

    /**
     * Used for the scan_type parameter to the getScanCost() routine.
     * STORECOST_SCAN_NORMAL indicates that the scan will use the standard
     * next/fetch, where each fetch can retrieve 1 or many rows (if
     * fetchNextGroup() interface is used).
     */
    int STORECOST_SCAN_SET=0x01;


    /**
     * Used for the scan_type parameter to the getScanCost() routine.
     * STORECOST_SCAN_SET - The entire result set will be retrieved using the
     * the fetchSet() interface.
     */
    int STORECOST_SCAN_NORMAL=0x02;

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
     * Return the cost of calling ConglomerateController.fetch().
     * <p/>
     * Return the estimated cost of calling ConglomerateController.fetch()
     * on the current conglomerate.  This gives the cost of finding a record
     * in the conglomerate given the exact RowLocation of the record in
     * question.
     * <p/>
     * The validColumns parameter describe what kind of row
     * is being fetched, ie. it may be cheaper to fetch a partial row than a
     * complete row.
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
     *                     performing to the ConglomerateController.
     *                     <p/>
     *                     STORECOST_CLUSTERED - The location of one fetch
     *                     is likely clustered "close" to the next
     *                     fetch.  For instance if the query plan were
     *                     to sort the RowLocations of a heap and then
     *                     use those RowLocations sequentially to
     *                     probe into the heap, then this flag should
     *                     be specified.  If this flag is not set then
     *                     access to the table is assumed to be
     *                     random - ie. the type of access one gets
     *                     if you scan an index and probe each row
     *                     in turn into the base table is "random".
     * @return The cost of the fetch.
     * @throws StandardException Standard exception policy.
     * @see RowUtil
     */
    void getFetchFromRowLocationCost(BitSet validColumns,
                                     int access_type,
                                     CostEstimate cost) throws StandardException;

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


    long getConglomerateAvgRowWidth();

    long getBaseTableAvgRowWidth();

    double getLocalLatency();

    double getRemoteLatency();

    int getNumPartitions();

    double conglomerateColumnSizeFactor(BitSet validColumns);

    double baseTableColumnSizeFactor(BitSet validColumns);

}
