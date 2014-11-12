package com.splicemachine.derby.impl.store.access.hbase;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.services.sanity.SanityManager;
import org.apache.derby.iapi.error.StandardException; 
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StoreCostResult;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.sql.compile.SortState;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceGenericCostController;
import com.splicemachine.derby.impl.store.access.base.SpliceScan;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.util.SortedSet;


public class HBaseCostController extends SpliceGenericCostController implements StoreCostController {
	private static final Logger LOG = Logger.getLogger(HBaseCostController.class);
	private OpenSpliceConglomerate open_conglom;

	public HBaseCostController(OpenSpliceConglomerate    open_conglom) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "init with open_conglom=%s",open_conglom);
    	this.open_conglom = open_conglom;
    }

    @Override
    public void getFetchFromRowLocationCost(
    FormatableBitSet      validColumns,
    int         access_type, CostEstimate costEstimate) throws StandardException {
    	SpliceLogUtils.trace(LOG, "getFetchFromRowLocation input {conglomerate=%s, costEstimate=%s",open_conglom, costEstimate);     		
    	double cost = SpliceConstants.fetchFromRowLocationCost*costEstimate.rowCount();
    	costEstimate.setEstimatedCost(costEstimate.getEstimatedCost() + cost);
    	SpliceLogUtils.trace(LOG, "getFetchFromRowLocation output {conglomerate=%s, costEstimate=%s",open_conglom, costEstimate);
    }

    @Override
	public void getScanCost(
    int                     scanType,
    long                    rowCount,
    int                     groupSize,
    boolean                 forUpdate,
    FormatableBitSet        scanColumnList,
    DataValueDescriptor[]   template,
    DataValueDescriptor[]   startKeyValue,
    int                     startSearchOperator,
    DataValueDescriptor[]   stopKeyValue,
    int                     stopSearchOperator,
    boolean                 reopenScan,
    int                     accessType,
    StoreCostResult         cost_result) throws StandardException {
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "getScanCost {scan_type=%d, row_count=%d, group_size=%d, "
    				+ "forUpdate=%s, scanColumnList=%s, template=%s, startKeyValue=%s, startSearchOperator=%d"
    				+ "stopKeyValue=%s, stopSearchOperator=%d, reopen_scan=%s, access_type=%d",
    				scanType, rowCount, groupSize, forUpdate, scanColumnList, template, startKeyValue, startSearchOperator,
    				stopKeyValue, stopSearchOperator, reopenScan, accessType);
    	SpliceScan spliceScan = new SpliceScan(open_conglom,scanColumnList,startKeyValue,startSearchOperator,null,stopKeyValue,stopSearchOperator,open_conglom.getTransaction(),false);
    	spliceScan.setupScan();
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG, "getScanCost generated Scan %s",spliceScan.getScan());		

		
		// Splice approach: scale Derby's calculation by the number of regions for
        // table in HBase

        SortedSet<Pair<HRegionInfo,ServerName>> regions = getRegions(open_conglom.getConglomerate().getContainerid());
    	((SortState) cost_result).setNumberOfRegions(regions==null?0:regions.size());
    	long estimatedRowCount = derbyFactory.computeRowCount(LOG, open_conglom.getConglomerate().getContainerid()+"", regions, spliceScan.getScan());
        double cost = estimatedRowCount*SpliceConstants.baseTablePerRowCost;
        if (SanityManager.DEBUG) {
            SanityManager.ASSERT(cost >= 0);
            SanityManager.ASSERT(estimatedRowCount >= 0);
        }        
        cost_result.setEstimatedCost(cost);
        cost_result.setEstimatedRowCount(estimatedRowCount);
        SpliceLogUtils.trace(LOG, "getScanCost output costResult=%s",cost_result);
        return;
    }
	
    public void close()
        throws StandardException {
    	 //if (open_conglom != null)
         //    open_conglom.close();
    }

    @Override
    public void getFetchFromFullKeyCost(
    FormatableBitSet     validColumns,
    int         access_type, CostEstimate costEstimate)
		throws StandardException {
    	SpliceLogUtils.trace(LOG, "getFetchFromFullKeyCost input {conglomerate=%s, costEstimate=%s",open_conglom, costEstimate);
    	costEstimate.setCost(SpliceConstants.getBaseTableFetchFromFullKeyCost, 1.0d, 1.0d);
    	SpliceLogUtils.trace(LOG, "getFetchFromFullKeyCost output {conglomerate=%s, costEstimate=%s",open_conglom, costEstimate);
    }

	public RowLocation newRowLocationTemplate()
		throws StandardException {
		return null;
	}
	
}
