/*

   Derby - Class org.apache.derby.impl.store.access.heap.HeapCostController

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

package com.splicemachine.derby.impl.store.access.btree;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;

import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StoreCostResult;
import org.apache.derby.iapi.store.access.conglomerate.Conglomerate;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceGenericCostController;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.utils.SpliceLogUtils;


/**

The StoreCostController interface provides methods that an access client
(most likely the system optimizer) can use to get store's estimated cost of
various operations on the conglomerate the StoreCostController was opened
for.
<p>
It is likely that the implementation of StoreCostController will open 
the conglomerate and will leave the conglomerate open until the
StoreCostController is closed.  This represents a significant amount of
work, so the caller if possible should attempt to open the StoreCostController
once per unit of work and rather than close and reopen the controller.  For
instance if the optimizer needs to cost 2 different scans against a single
conglomerate, it should use one instance of the StoreCostController.
<p>
The locking behavior of the implementation of a StoreCostController is
undefined, it may or may not get locks on the underlying conglomerate.  It
may or may not hold locks until end of transaction.  
An optimal implementation will not get any locks on the underlying 
conglomerate, thus allowing concurrent access to the table by a executing
query while another query is optimizing.
<p>
The StoreCostController gives 2 kinds of cost information

**/

public class IndexCostController extends SpliceGenericCostController implements StoreCostController {
    private static final Logger LOG = Logger.getLogger(IndexCostController.class);
	private OpenSpliceConglomerate open_conglom;
	private Conglomerate baseConglomerate;
	
    public IndexCostController(OpenSpliceConglomerate open_conglom) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "init with open_conglom=%s",open_conglom);
    	this.open_conglom = open_conglom;
    	this.baseConglomerate = ((SpliceTransactionManager) open_conglom.getTransactionManager()).findConglomerate(open_conglom.getIndexConglomerate());
    }

    /**
     * Return the cost of calling ConglomerateController.fetch().
     * <p>
     * Return the estimated cost of calling ConglomerateController.fetch()
     * on the current conglomerate.  This gives the cost of finding a record
     * in the conglomerate given the exact RowLocation of the record in
     * question. 
     * <p>
     * The validColumns describes what kind of row is being fetched, 
     * ie. it may be cheaper to fetch a partial row than a complete row.
     * <p>
     *
     *
	 * @param validColumns    A description of which columns to return from
     *                        row on the page into "templateRow."  templateRow,
     *                        and validColumns work together to
     *                        describe the row to be returned by the fetch - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a fetched 
     *                        "row".
     *
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ConglomerateController.  
     *
     *                        STORECOST_CLUSTERED - The location of one fetch
     *                            is likely clustered "close" to the next 
     *                            fetch.  For instance if the query plan were
     *                            to sort the RowLocations of a heap and then
     *                            use those RowLocations sequentially to 
     *                            probe into the heap, then this flag should
     *                            be specified.  If this flag is not set then
     *                            access to the table is assumed to be
     *                            random - ie. the type of access one gets 
     *                            if you scan an index and probe each row
     *                            in turn into the base table is "random".
     *
     *
	 * @return The cost of the fetch.
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see org.apache.derby.iapi.store.access.RowUtil
     **/
    @Override
    public double getFetchFromRowLocationCost(
    FormatableBitSet validColumns,int access_type)
		throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "getFetchFromRowLocation {conglomerate=%s, validColumns=%s, accessType=%d",
    				open_conglom, validColumns==null?"null":validColumns.toString(),access_type);
    	return SpliceConstants.fetchFromRowLocationCost;
    }

    /**
     * Calculate the cost of a scan.
     * <p>
     * Cause this object to calculate the cost of performing the described
     * scan.  The interface is setup such that first a call is made to
     * calcualteScanCost(), and then subsequent calls to accessor routines
     * are made to get various pieces of information about the cost of
     * the scan.
     * <p>
     * For the purposes of costing this routine is going to assume that 
     * a page will remain in cache between the time one next()/fetchNext()
     * call and a subsequent next()/fetchNext() call is made within a scan.
     * <p>
     * The result of costing the scan is placed in the "cost_result".  
     * The cost of the scan is stored by calling 
     * cost_result.setEstimatedCost(cost).
     * The estimated row count is stored by calling 
     * cost_result.setEstimatedRowCount(row_count).
     * <p>
     * The estimated cost of the scan assumes the caller will 
     * execute a fetchNext() loop for every row that qualifies between
     * start and stop position.  Note that this cost is different than
     * execution a next(),fetch() loop; or if the scan is going to be
     * terminated by client prior to reaching the stop condition.
     * <p>
     * The estimated number of rows returned from the scan 
     * assumes the caller will execute a fetchNext() loop for every 
     * row that qualifies between start and stop position.
     * <p>
     *
     *
     * @param scan_type       The type of scan that will be executed.  There
     *                        are currently 2 types:
     *                        STORECOST_SCAN_NORMAL - scans will be executed
     *                        using the standard next/fetch, where each fetch
     *                        can retrieve 1 or many rows (if fetchNextGroup()
     *                        interface is used).
     *
     *                        STORECOST_SCAN_SET - The entire result set will
     *                        be retrieved using the the fetchSet() interface.
     *
     * @param row_count       Estimated total row count of the table.  The 
     *                        current system tracks row counts in heaps better
     *                        than btree's (btree's have "rows" which are not
     *                        user rows - branch rows, control rows), so 
     *                        if available the client should
     *                        pass in the base table's row count into this
     *                        routine to be used as the index's row count.
     *                        If the caller has no idea, pass in -1.
     *
     * @param group_size      The number of rows to be returned by a single
     *                        fetch call for STORECOST_SCAN_NORMAL scans.
     *
	 * @param forUpdate       Should be true if the caller intends to update 
     *                        through the scan.
     * 
	 * @param scanColumnList  A description of which columns to return from 
     *                        every fetch in the scan.  template, 
     *                        and scanColumnList work together
     *                        to describe the row to be returned by the scan - 
     *                        see RowUtil for description of how these three 
     *                        parameters work together to describe a "row".
     * 
     * @param template        A prototypical row which the scan may use to
	 *                        maintain its position in the conglomerate.  Not 
     *                        all access method scan types will require this, 
     *                        if they don't it's ok to pass in null.
     *                        In order to scan a conglomerate one must 
     *                        allocate 2 separate "row" templates.  The "row" 
     *                        template passed into openScan is for the private
     *                        use of the scan itself, and no access to it
     *                        should be made by the caller while the scan is 
     *                        still open.  Because of this the scanner must 
     *                        allocate another "row" template to hold the 
     *                        values returned from fetch().  Note that this 
     *                        template must be for the full row, whether a 
     *                        partial row scan is being executed or not.
     *
	 * @param startKeyValue   An indexable row which holds a (partial) key 
     *                        value which, in combination with the 
     *                        startSearchOperator, defines the starting 
     *                        position of the scan.  If null, the starting
     *                        position of the scan is the first row of the 
     *                        conglomerate.  The startKeyValue must only
     *                        reference columns included in the scanColumnList.
     *
	 * @param startSearchOperator 
     *                        an operator which defines how the startKeyValue
     *                        is to be searched for.  If startSearchOperation 
     *                        is ScanController.GE, the scan starts on the 
     *                        first row which is greater than or equal to the 
	 *                        startKeyValue.  If startSearchOperation is 
     *                        ScanController.GT, the scan starts on the first
     *                        row whose key is greater than startKeyValue.  The
     *                        startSearchOperation parameter is ignored if the
     *                        startKeyValue parameter is null.
     *
	 * @param stopKeyValue    An indexable row which holds a (partial) key 
     *                        value which, in combination with the 
     *                        stopSearchOperator, defines the ending position
     *                        of the scan.  If null, the ending position of the
     *                        scan is the last row of the conglomerate.  The
     *                        stopKeyValue must only reference columns included
     *                        in the scanColumnList.
     *
	 * @param stopSearchOperator
     *                        an operator which defines how the stopKeyValue
     *                        is used to determine the scan stopping position. 
     *                        If stopSearchOperation is ScanController.GE, the
     *                        scan stops just before the first row which is
     *                        greater than or equal to the stopKeyValue.  If 
     *                        stopSearchOperation is ScanController.GT, the 
     *                        scan stops just before the first row whose key 
     *                        is greater than startKeyValue.  The
     *                        stopSearchOperation parameter is ignored if the
     *                        stopKeyValue parameter is null.
     *
     *                        
     * @param access_type     Describe the type of access the query will be
     *                        performing to the ScanController.  
     *
     *                        STORECOST_CLUSTERED - The location of one scan
     *                            is likely clustered "close" to the previous 
     *                            scan.  For instance if the query plan were
     *                            to used repeated "reopenScan()'s" to probe
     *                            for the next key in an index, then this flag
     *                            should be be specified.  If this flag is not 
     *                            set then each scan will be costed independant
     *                            of any other predicted scan access.
     *
     *
	 * @exception  StandardException  Standard exception policy.
     *
	 * @see org.apache.derby.iapi.store.access.RowUtil
     **/
    @Override
	public void getScanCost(
    int                     scan_type,
    long                    row_count,
    int                     group_size,
    boolean                 forUpdate,
    FormatableBitSet        scanColumnList,
    DataValueDescriptor[]   template,
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator,
    boolean                 reopen_scan,
    int                     access_type,
    StoreCostResult         cost_result) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "getScanCost {scan_type=%d, row_count=%d, group_size=%d, "
    				+ "forUpdate=%s, scanColumnList=%s, template=%s, startKeyValue=%s, startSearchOperator=%d"
    				+ "stopKeyValue=%s, stopSearchOperator=%d, reopen_scan=%s, access_type=%d",
    				scan_type, row_count, group_size, forUpdate, scanColumnList, template==null?"null":Arrays.toString(template), startKeyValue, startSearchOperator,
    				stopKeyValue, stopSearchOperator, reopen_scan, access_type);
    	assert open_conglom !=null;
//    	SpliceScan spliceScan = new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,null,stopKeyValue,stopSearchOperator,open_conglom.getTransaction(),false);
//		spliceScan.setupScan();
  //  	if (LOG.isTraceEnabled())
//    		SpliceLogUtils.trace(LOG, "getScanCost generated Scan %s",spliceScan.getScan());		
		if (SanityManager.DEBUG) {
            SanityManager.ASSERT(
                scan_type == StoreCostController.STORECOST_SCAN_NORMAL ||
                scan_type == StoreCostController.STORECOST_SCAN_SET);
        }

        SortedSet<HRegionInfo> baseRegions = getRegions(baseConglomerate.getContainerid());
        Map<String,RegionLoad> baseRegionLoads = HBaseRegionLoads.getCachedRegionLoadsMapForTable(baseConglomerate.getContainerid()+"");
        long estimatedRowCount = computeRowCount(baseRegions,baseRegionLoads,SpliceConstants.hbaseRegionRowEstimate,SpliceConstants.regionMaxFileSize,null);    
        double cost = estimatedRowCount*SpliceConstants.indexPerRowCost*(1+.001*open_conglom.getFormatIds().length); // Attempt to make bigger indexes / tables cost more.
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(cost >= 0);
            SanityManager.ASSERT(estimatedRowCount >= 0);
        }
        cost_result.setEstimatedCost(cost);
        cost_result.setEstimatedRowCount(estimatedRowCount);
        return;
    }
	
	@Override
	public double getFetchFromFullKeyCost(FormatableBitSet validColumns, int access_type) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "getFetchFromFullKeyCost {conglomerate=%s, validColumns=%s, accessType=%d",
    				open_conglom, validColumns==null?"null":validColumns.toString(),access_type);
		return SpliceConstants.getIndexFetchFromFullKeyCost;
	}
}
