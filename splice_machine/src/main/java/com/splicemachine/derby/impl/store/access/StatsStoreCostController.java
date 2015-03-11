package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.store.access.conglomerate.GenericController;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.TableStatistics;
import com.splicemachine.stats.estimate.Distribution;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A Cost Controller which uses underlying Statistics information to estimate the cost of a scan.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public class StatsStoreCostController extends GenericController implements StoreCostController {
    protected TableStatistics conglomerateStatistics;
    protected OpenSpliceConglomerate baseConglomerate;

    public StatsStoreCostController(OpenSpliceConglomerate baseConglomerate) throws StandardException {
        this.baseConglomerate = baseConglomerate;
        BaseSpliceTransaction bst = (BaseSpliceTransaction)baseConglomerate.getTransaction();
        TxnView txn = bst.getActiveStateTxn();
        long conglomId = baseConglomerate.getConglomerate().getContainerid();

        try {
            this.conglomerateStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, conglomId);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void close() throws StandardException {

    }

    @Override
    public long getEstimatedRowCount() throws StandardException {
        return conglomerateStatistics.rowCount();
    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet validColumns, int access_type, CostEstimate cost) throws StandardException {
        /*
         * This is useful when estimating the cost of an index lookup. We approximate it by using
         * Remote Latency
         */
        cost.setEstimatedCost(cost.getEstimatedCost()+ conglomerateStatistics.remoteReadLatency()*cost.rowCount());
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
        cost.setEstimatedCost(cost.getEstimatedCost() + conglomerateStatistics.remoteReadLatency());
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
        estimateCost(conglomerateStatistics,
                startKeyValue, startSearchOperator,
                stopKeyValue, stopSearchOperator, baseConglomerate.getColumnOrdering(), (CostEstimate) cost_result,false);
    }

    @Override
    public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {
        //TODO -sf- implement!
        costEstimate.setCost(costEstimate.getEstimatedCost()* SpliceConstants.extraQualifierMultiplier,
                (double) costEstimate.getEstimatedRowCount()*SpliceConstants.extraQualifierMultiplier,
                costEstimate.singleScanRowCount()*SpliceConstants.extraQualifierMultiplier);
    }

    @Override public RowLocation newRowLocationTemplate() throws StandardException { return null; }

    @SuppressWarnings("unchecked")
    protected void estimateCost(TableStatistics stats,
                                DataValueDescriptor[] startKeyValue,
                                int startSearchOperator,
                                DataValueDescriptor[] stopKeyValue,
                                int stopSearchOperator,
                                int[] keyMap,
                                CostEstimate costEstimate,
                                boolean scalePartition) {
            /*
             * We need to estimate the scan selectivity.
             *
             * Unfortunately, we don't have multi-dimensional statistics (e.g. the statistics
             * for pred(A) and pred(B)), so we can't do this easily. Instead, we use a fairly
             * straightforward heuristic.
             *
             * First, we count the number of rows which match the first predicate. Because we
             * use this to construct the row key range on the sorted table, we can say that
             * this is the maximum number of rows that we are going to visit. Then we compute the
             * number of rows that match the second predicate. That number is independent of
             * the number of rows matching the first predicate, so we have two numbers: N1 and N2.
             * We know that condition 1 has to hold, so N2 *should* be strictly less. However, we don't really
             * know that that's true--the data could be elsewhere. So we assume that the distribution is
             * evenly in or out, and the number of rows is then N2/size(partition)*N1. We then repeat
             * this process recursively, to reduce the total number of rows down. Not as good a heuristic
             * as maintaining a distribution of row keys directly, but for now it'll do.
             */
        List<PartitionStatistics> partitionStatistics = stats.partitionStatistics();
        boolean includeStop = stopSearchOperator == ScanController.GT;
        boolean includeStart = startSearchOperator != ScanController.GT;
        long numRows = 0l;
        double cost = 0d;
        int numPartitions = 0;
        for (PartitionStatistics partStats : partitionStatistics) {
            long count = count(partStats,
                    startKeyValue, includeStart,
                    stopKeyValue, includeStop,
                    0, partStats.rowCount(), keyMap);
            numRows += count;
            if (count > 0) {
                numPartitions += 1;
                double l = count * partStats.localReadLatency();
                if(scalePartition)
                    cost += l*costScaleFactor(partStats);
                else
                    cost+=l;
            }
        }
        costEstimate.setEstimatedRowCount(numRows);
        costEstimate.setEstimatedCost(cost);
        costEstimate.setNumPartitions(numPartitions);
    }

    protected double costScaleFactor(PartitionStatistics partStats) {
        return 1d;
    }

    private long count(PartitionStatistics partStats,
                       DataValueDescriptor[] startKeyValue,
                       boolean includeStart,
                       DataValueDescriptor[] stopKeyValue,
                       boolean includeStop,
                       int keyPosition,
                       long currentCount,
                       int[] keyMap) {
        if(startKeyValue==null){
            if(stopKeyValue==null) return currentCount;
            else if(keyPosition>=stopKeyValue.length) return currentCount;
        }else if(keyPosition>=startKeyValue.length) return currentCount;
        DataValueDescriptor start = startKeyValue==null? null: startKeyValue[keyPosition];
        DataValueDescriptor stop = stopKeyValue==null?null: stopKeyValue[keyPosition];

        int columnId = keyMap[keyPosition];
        Distribution<DataValueDescriptor> dist = partStats.columnDistribution(columnId);
        long c = dist.rangeSelectivity(start, stop, includeStart, includeStop);
        if(c>0){
            double size = partStats.totalSize();
            double estCount;
            if(keyPosition==0)
                estCount = c;
            else
                estCount = c*currentCount/size;
            return count(partStats,startKeyValue,includeStart,stopKeyValue,includeStop,keyPosition+1,(long)estCount,keyMap);
        }else
            return 0l;
    }
}
