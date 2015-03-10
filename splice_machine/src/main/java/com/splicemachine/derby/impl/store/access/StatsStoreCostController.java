package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.TableStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.utils.Pair;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.store.access.ScanController;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StoreCostResult;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.store.access.conglomerate.GenericController;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;

/**
 * A Cost Controller which uses underlying Statistics information to estimate the cost of a scan.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public class StatsStoreCostController extends GenericController implements StoreCostController{
    private TableStatistics tableStatistics;
    private OpenSpliceConglomerate baseConglomerate;

    public StatsStoreCostController(OpenSpliceConglomerate baseConglomerate) throws StandardException {
        this.baseConglomerate = baseConglomerate;
        BaseSpliceTransaction bst = (BaseSpliceTransaction)baseConglomerate.getTransaction();
        TxnView txn = bst.getActiveStateTxn();
        long conglomId = baseConglomerate.getConglomerate().getContainerid();

        try {
            this.tableStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, conglomId);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException {

    }

    @Override
    public long getEstimatedRowCount() throws StandardException {
        return tableStatistics.rowCount();
    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet validColumns, int access_type, CostEstimate cost) throws StandardException {
        /*
         * This is useful when estimating the cost of an index lookup. We approximate it by using
         * Remote Latency
         */
        cost.setEstimatedCost(cost.getEstimatedCost()+tableStatistics.remoteReadLatency()*cost.rowCount());
    }

    @Override
    public void getFetchFromFullKeyCost(FormatableBitSet validColumns, int access_type, CostEstimate cost) throws StandardException {
        /*
         * This is the cost to read a single row from a PK or indexed table (without any associated index lookups).
         * Since we know the full key, we have two scenarios:
         *
         * 1. The table is unique (i.e. a primary key or unique index lookup)
         * 2. The table is not unique (i.e. non-unique index, grouped tables, etc.)
         *
         * In the first case, the cost is just remoteReadLatency(), since we know we will only be scanning
         * a single row. In the second case, however, it's the cost of scanning however many rows match the query.
         *
         * The first scenario uses this method, but the second scenario actually uses getScanCost(), so we're safe
         * assuming just a single row here
         */
        cost.setEstimatedCost(cost.getEstimatedCost()+tableStatistics.remoteReadLatency());
        cost.setEstimatedRowCount(1l);
    }

    @Override
    public void getScanCost(int scan_type,
                            long row_count,
                            int group_size,
                            boolean forUpdate,
                            FormatableBitSet scanColumnList,
                            DataValueDescriptor[] template,
                            DataValueDescriptor[] startKeyValue,
                            int startSearchOperator,
                            DataValueDescriptor[] stopKeyValue,
                            int stopSearchOperator,
                            boolean reopen_scan,
                            int access_type,
                            StoreCostResult cost_result) throws StandardException {
       /*
        * We estimate the cost of the key scan. This is effectively the selectivity of the
        * keys, times the cost to read a single row.
        *
        * TODO -sf- at some point,we will need to make a distinction between performing a local
        * scan and a remote scan. For now, we will opt for remote scan always.
        *
        * Keys are handed in according to derby's rules. We may not have a start key (if the scan is
        * PK1 < value), and we may not have an end key (if the scan is PK1 > value). In these cases,
        * the selectivity is rangeSelectivity(start,null) or rangeSelectivity(null,stop). The column
        * id for the keys is located in the conglomerate.
        */
        if(startKeyValue==null && stopKeyValue==null){
            /*
             * There are no keys to scan, so we are doing a full table scan. We estimate
             * that cost in a sequential manner, by taking rowCount*localReadLatency()
             *
             * TODO -sf- make a distinction about parallel reads here based on the fact that there
             * may be multiple partitions
             */
            cost_result.setEstimatedRowCount(tableStatistics.rowCount());
            cost_result.setEstimatedCost(tableStatistics.rowCount()*tableStatistics.remoteReadLatency());
            return;
        }
        int[] keyOrdering = baseConglomerate.getColumnOrdering();
        boolean[] sortOrdering = baseConglomerate.getAscDescInfo();

        Pair<DataValueDescriptor,DataValueDescriptor>[] columnRanges;
        boolean[] startInclusion;
        boolean[] stopInclusion;
        if(startKeyValue==null){
            //we have no start keys
            columnRanges = new Pair[stopKeyValue.length];
            startInclusion = new boolean[stopKeyValue.length];
            stopInclusion = new boolean[stopKeyValue.length];
            for(int i=0;i<columnRanges.length;i++){
                columnRanges[i] = Pair.newPair(null,null);
                startInclusion[i] = true;
            }
        }else{
            columnRanges = new Pair[startKeyValue.length];
            startInclusion = new boolean[startKeyValue.length];
            stopInclusion = new boolean[startKeyValue.length];
            boolean includeStart = startSearchOperator != ScanController.GT;
            for(int i=0;i<columnRanges.length;i++){
                columnRanges[i] = Pair.newPair(startKeyValue[i],null);
                startInclusion[i] = includeStart;
            }
        }

        if(stopKeyValue==null){
            for(int i=0;i<columnRanges.length;i++){
                columnRanges[i].setSecond(null);
                stopInclusion[i] = true;
            }
        }else{
            boolean includeStop = stopSearchOperator==ScanController.GT;
            Arrays.fill(stopInclusion, includeStop);
        }

        long numRows = tableStatistics.rowCount();
        for(int i=0;i<columnRanges.length;i++){
            DataValueDescriptor start = columnRanges[i].getFirst();
            DataValueDescriptor stop = columnRanges[i].getSecond();
            boolean includeStart = startInclusion[i];
            boolean includeStop = stopInclusion[i];

            Distribution<DataValueDescriptor> columnDistribution =
                                                   tableStatistics.columnDistribution(keyOrdering[i]);
            long nR = columnDistribution.rangeSelectivity(start,stop,includeStart,includeStop);
            numRows = Math.min(numRows,nR);
        }

        /*
         * numRows contains how many rows we are going to touch, so now just set a cost as
         * remoteReadLatency()*numRows
         */
        cost_result.setEstimatedRowCount(numRows);
        cost_result.setEstimatedCost(numRows*tableStatistics.remoteReadLatency());
    }

    @Override
    public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {
        //TODO -sf- implement!
        costEstimate.setCost(costEstimate.getEstimatedCost()* SpliceConstants.extraQualifierMultiplier,
                (double) costEstimate.getEstimatedRowCount()*SpliceConstants.extraQualifierMultiplier,
                costEstimate.singleScanRowCount()*SpliceConstants.extraQualifierMultiplier);
    }

    @Override public RowLocation newRowLocationTemplate() throws StandardException { return null; }
}
