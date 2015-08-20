/*

   Derby - Class com.splicemachine.db.impl.store.access.btree.BTreeCostController

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

package com.splicemachine.db.impl.store.access.btree;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.DynamicCompiledOpenConglomInfo;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.iapi.store.access.conglomerate.Conglomerate;
import com.splicemachine.db.iapi.store.access.conglomerate.LogicalUndo;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.ContainerHandle;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;

import java.util.BitSet;
import java.util.List;
import java.util.Properties;

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
 * @see com.splicemachine.db.iapi.store.access.TransactionController#openStoreCost
 */

public class BTreeCostController extends OpenBTree implements StoreCostController{

    // 1.5 numbers on mikem old machine:
    //
    // The magic numbers are based on the following benchmark results:
    //
    //                                         no col   one int col  all cols
    //                                         ------   -----------  --------
    //100 byte heap fetch by row loc, cached   0.3625     0.5098     0.6629
    //100 byte heap fetch by row loc, uncached 1.3605769  1.5168269  1.5769231
    //4 byte   heap fetch by row loc, cached   0.3745     0.4016     0.3766
    //4 byte   heap fetch by row loc, uncached 4.1938777  3.5714285  4.4897957
    //
    //                                 no col    one int col  all cols
    //                                 ------    -----------  --------
    //Int col one level btree
    //  fetch by exact key, cached     0.781     1.012         0.42
    //  fetch by exact key, sort merge 1.081     1.221         0.851
    //  fetch by exact key, uncached   0.0       0.0           0.0
    //Int col two level btree
    //  fetch by exact key, cached     1.062     1.342         0.871
    //  fetch by exact key, sort merge 1.893     2.273         1.633
    //  fetch by exact key, uncached   5.7238097 5.3428574     4.7714286
    //String key one level btree
    //  fetch by exact key, cached     1.082     0.811         0.781
    //  fetch by exact key, sort merge 1.572     1.683         1.141
    //  fetch by exact key, uncached   0.0       0.0           0.0
    //String key two level btree
    //  fetch by exact key, cached     2.143     2.664         1.953
    //  fetch by exact key, sort merge 3.775     4.116         3.505
    //  fetch by exact key, uncached   4.639474  5.0052633     4.4289474

    // mikem new machine - insane, codeline, non-jit 1.1.7 numbers
    //
    //                                         no col   one int col  all cols
    //                                         ------   -----------  --------
    //100 byte heap fetch by row loc, cached   0.1662    0.4597      0.5618
    //100 byte heap fetch by row loc, uncached 0.7565947 1.2601918   1.6690648
    //4 byte   heap fetch by row loc, cached   0.1702    0.1983      0.1903
    //4 byte   heap fetch by row loc, uncached 1.5068493 1.3013699   1.6438357
    //
    //                                 no col    one int col  all cols
    //                                 ------    -----------  --------
    // Int col one level btree
    //   fetch by exact key, cached     0.271    0.511        0.33
    //   fetch by exact key, sort merge 0.691    0.921        0.771
    //   fetch by exact key, uncached   0.0      0.0          0.0
    // Int col two level btree
    //   fetch by exact key, cached     0.541    0.711        0.561
    //   fetch by exact key, sort merge 1.432    1.682        1.533
    //   fetch by exact key, uncached   3.142857 3.6285715    3.2380953
    // String key one level btree
    //   fetch by exact key, cached     0.611    0.851        0.701
    //   fetch by exact key, sort merge 1.051    1.272        1.122
    //   fetch by exact key, uncached   0.0      0.0          0.0
    // String key two level btree
    //   fetch by exact key, cached     1.532    1.843        1.622
    //   fetch by exact key, sort merge 2.844    3.155        2.984
    //   fetch by exact key, uncached   3.4      3.636842     3.531579
    // 


    // The following costs are search costs to find a row on a leaf, use
    // the heap costs to determine scan costs, for now ignore qualifier 
    // application and stop comparisons.
    // I used the int key, 2 level numbers divided by 2 to get per level.

    private static final double
            BTREE_CACHED_FETCH_BY_KEY_PER_LEVEL=(0.541/2);

    private static final double
            BTREE_SORTMERGE_FETCH_BY_KEY_PER_LEVEL=(1.432/2);

    private static final double
            BTREE_UNCACHED_FETCH_BY_KEY_PER_LEVEL=(3.143/2);

    // saved values passed to init().
    TransactionManager init_xact_manager;
    Transaction init_rawtran;
    Conglomerate init_conglomerate;

    /**
     * Only lookup these estimates from raw store once.
     */
    long num_pages;
    long num_rows;
    long page_size;
    int tree_height;

    /* Constructors for This class: */

    public BTreeCostController(){
    }

    /* Private/Protected methods of This class: */

    /**
     * Initialize the cost controller.
     * <p/>
     * Save initialize parameters away, and open the underlying container.
     * <p/>
     *
     * @param xact_manager access manager transaction.
     * @param rawtran      Raw store transaction.
     * @throws StandardException Standard exception policy.
     */
    public void init(
            TransactionManager xact_manager,
            BTree conglomerate,
            Transaction rawtran)
            throws StandardException{
        super.init(
                xact_manager,
                xact_manager,
                (ContainerHandle)null,         // open the btree.
                rawtran,
                false,
                ContainerHandle.MODE_READONLY,
                TransactionManager.MODE_NONE,
                (BTreeLockingPolicy)null,      // RESOLVE (mikem) - this means
                // no locks during costing - will
                // that work?????
                conglomerate,
                (LogicalUndo)null,             // read only, so no undo necessary
                (DynamicCompiledOpenConglomInfo)null);

        // look up costs from raw store.  For btrees these numbers are out
        // of whack as they want to be leaf specific numbers but they include
        // every page branch and leafs.
        num_pages=this.container.getEstimatedPageCount(/* unused flag */ 0);

        // subtract one row for every page to account for internal control row
        // which exists on every page.
        num_rows=
                this.container.getEstimatedRowCount(/*unused flag*/ 0)-num_pages;

        Properties prop=new Properties();
        prop.put(Property.PAGE_SIZE_PARAMETER,"");
        this.container.getContainerProperties(prop);
        page_size=
                Integer.parseInt(prop.getProperty(Property.PAGE_SIZE_PARAMETER));

        tree_height=getHeight();

        return;
    }

    /* Public Methods of This class: */

    /**
     * Close the controller.
     * <p/>
     * Close the open controller.  This method always succeeds, and never
     * throws any exceptions. Callers must not use the StoreCostController
     * Cost controller after closing it; they are strongly advised to clear
     * out the scan controller reference after closing.
     * <p/>
     */
    public void close()
            throws StandardException{
        super.close();
    }

    /**
     * Return the cost of calling ConglomerateController.fetch().
     * <p/>
     * Return the estimated cost of calling ConglomerateController.fetch()
     * on the current conglomerate.  This gives the cost of finding a record
     * in the conglomerate given the exact RowLocation of the record in
     * question.
     * <p/>
     * The validColumns parameter describes what kind of row
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
     * @see com.splicemachine.db.iapi.store.access.RowUtil
     */
    public void getFetchFromRowLocationCost(
            BitSet validColumns,
            int access_type,CostEstimate costEstimate)
            throws StandardException{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

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
     * @see com.splicemachine.db.iapi.store.access.RowUtil
     */
    public void getFetchFromFullKeyCost(
            BitSet validColumns,
            int access_type,CostEstimate costEstimate)
            throws StandardException{
        throw new RuntimeException("not implemented");
    }


    @Override
    public double getSelectivity(int columnNumber,DataValueDescriptor start,boolean includeStart,DataValueDescriptor stop,boolean includeStop){
        return 1.0d;
    }

    @Override
    public double nullSelectivity(int columnNumber){
        return 1.0d;
    }

    /**
     * Return an "empty" row location object of the correct type.
     * <p/>
     *
     * @return The empty Rowlocation.
     * @throws StandardException Standard exception policy.
     */
    public RowLocation newRowLocationTemplate()
            throws StandardException{
        throw StandardException.newException(
                SQLState.BTREE_UNIMPLEMENTED_FEATURE);
    }

    @Override
    public double rowCount(){
        return num_rows;
    }

    @Override
    public double nonNullCount(int columnNumber){
        return num_rows;
    }

    @Override
    public long cardinality(int columnNumber) {
        return num_rows;
    }

    @Override
    public long getConglomerateAvgRowWidth() {
        return 20;
    }

    @Override
    public long getBaseTableAvgRowWidth() {
        return 20;
    }

    @Override
    public double getLocalLatency() {
        return 1.0d;
    }

    @Override
    public double getRemoteLatency() {
        return 40.0d;
    }

    @Override
    public int getNumPartitions() {
        return 1;
    }

    @Override
    public double conglomerateColumnSizeFactor(BitSet validColumns) {
        return 1.0d;
    }

    @Override
    public double baseTableColumnSizeFactor(BitSet validColumns) {
        return 1.0d;
    }


}

