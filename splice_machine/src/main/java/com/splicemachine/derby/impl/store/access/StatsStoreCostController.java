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
import com.splicemachine.stats.ColumnStatistics;
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
        double scale = conglomerateStatistics.remoteReadLatency();
        cost.setEstimatedCost(cost.getEstimatedCost()+ scale*cost.rowCount());
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
        cost.setEstimatedCost(cost.getEstimatedCost() + conglomerateStatistics.localReadLatency());
        cost.setEstimatedRowCount(cost.getEstimatedRowCount()+1l);
        cost.setNumPartitions(1);
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
                startKeyValue,startSearchOperator,
                stopKeyValue,stopSearchOperator,baseConglomerate.getColumnOrdering(),(CostEstimate)cost_result);
    }

    @Override
    public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {
        //TODO -sf- implement!
        costEstimate.setCost(costEstimate.getEstimatedCost()*SpliceConstants.extraQualifierMultiplier,
                (double)costEstimate.getEstimatedRowCount()*SpliceConstants.extraQualifierMultiplier,
                costEstimate.singleScanRowCount()*SpliceConstants.extraQualifierMultiplier);
    }

    @Override public RowLocation newRowLocationTemplate() throws StandardException { return null; }

    @Override
    public double getSelectivity(int columnNumber,
                                 DataValueDescriptor start,boolean includeStart,
                                 DataValueDescriptor stop,boolean includeStop){
        return selectivityFraction(conglomerateStatistics,columnNumber,
                start,includeStart,
                stop,includeStop);
    }

    @Override
    public double nullSelectivity(int columnNumber){
        return nullSelectivityFraction(conglomerateStatistics,columnNumber);
    }

    protected double nullSelectivityFraction(TableStatistics stats,int columnNumber){
        List<PartitionStatistics> partStats = stats.partitionStatistics();
        long nullCount = 0l;
        int missingStatsCount = partStats.size();
        for(PartitionStatistics pStats:partStats){
            ColumnStatistics<DataValueDescriptor> cStats=pStats.columnStatistics(columnNumber-1);
            if(cStats!=null){
                nullCount+=cStats.nullCount();
                missingStatsCount--;
            }
        }
        double nc=0d;
        if(missingStatsCount==partStats.size()){
            /*
             * We have no statistics for this column, so we fall back on an arbitrarily configured
             * selectivity criteria
             */
            return SpliceConstants.extraQualifierMultiplier;
        }else if(missingStatsCount>0){
            /*
             * We have a situation where statistics are missing from some, but not all
             * partitions, for whatever reason. In that case, we fill in the missing regions
             * by assuming homogeneity--we assume that each missing partition contains roughly
             * the average nullCount of all the collected partitions. Thus, we find the
             * average, and multiply it by the number of missing regions
             */
            nc = ((double)nullCount)/(partStats.size()-missingStatsCount);
            nc*=missingStatsCount;
        }
        nc+=nullCount;
        return nc/stats.rowCount();
    }

    protected double selectivityFraction(TableStatistics stats,
                                         int columnNumber,
                                         DataValueDescriptor start,boolean includeStart,
                                         DataValueDescriptor stop,boolean includeStop){
        List<PartitionStatistics> partStats = stats.partitionStatistics();
        long rowCount = 0l;
        int missingStatsCount = partStats.size();
        for(PartitionStatistics pStats:partStats){
            ColumnStatistics<DataValueDescriptor> cStats = pStats.columnStatistics(columnNumber-1);
            if(cStats!=null){
                rowCount+= cStats.getDistribution().rangeSelectivity(start,stop,includeStart,includeStop);
                missingStatsCount--;
            }
        }
        double rc = 0d;
        if(missingStatsCount==partStats.size()){
            /*
             * we have no statistics for this column, so fall back to an abitrarily configured
             * selectivity criteria
             */
            return SpliceConstants.extraQualifierMultiplier;
        }else if(missingStatsCount>0){
            /*
             * We are missing some statistics, but not others. Fill in the missing
             * partitions with the average row count from all the other partitions
             */
            rc = ((double)rowCount)/(partStats.size()-missingStatsCount);
            rc*=missingStatsCount;
        }
        rc+=rowCount;
        return rc/stats.rowCount();
    }

    @SuppressWarnings("unchecked")
    protected void estimateCost(TableStatistics stats,
                                DataValueDescriptor[] startKeyValue,
                                int startSearchOperator,
                                DataValueDescriptor[] stopKeyValue,
                                int stopSearchOperator,
                                int[] keyMap,
                                CostEstimate costEstimate) {
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
        if(startKeyValue==null && stopKeyValue==null){
            /*
             * We have no start and stop keys, so the cost is
             * the cost of a full table scan
             */
            for(PartitionStatistics partStats:partitionStatistics){
                long rc = partStats.rowCount();
                if(rc>0){
                    numPartitions++;
                    cost +=partStats.localReadLatency()*rc;
                    numRows+=rc;
                }
            }
        }else{
            /*
             * We have either a start or a stop key, so we can compute a range
             */
            for(PartitionStatistics pStats:partitionStatistics){
                double selectivity = 1.0d;
                int size = startKeyValue!=null?startKeyValue.length :stopKeyValue.length;
                for(int i=0;i<size;i++){
                    DataValueDescriptor start = startKeyValue!=null?startKeyValue[i]:null;
                    DataValueDescriptor stop = stopKeyValue!=null?stopKeyValue[i]:null;
                    ColumnStatistics<DataValueDescriptor> cStats = pStats.columnStatistics(keyMap[i]);
                    if(cStats!=null){
                        double rc = cStats.getDistribution().rangeSelectivity(start,stop,includeStart,includeStop);
                        selectivity*=rc/cStats.nonNullCount();
                    }
                }
                long partRc = (long)Math.ceil(selectivity*pStats.rowCount());
                if(partRc>0){
                    numPartitions++;
                    numRows+=partRc;
                    cost+=pStats.localReadLatency()*partRc;
                }
            }
        }
//        for (PartitionStatistics partStats : partitionStatistics){
//            long count=count(partStats,
//                    startKeyValue,includeStart,
//                    stopKeyValue,includeStop,
//                    0,partStats.rowCount(),keyMap);
//            numRows+=count;
//            if(count>0){
//                numPartitions+=1;
//                cost+=count*partStats.localReadLatency();
//            }
//        }

        costEstimate.setEstimatedRowCount(numRows);
        costEstimate.setEstimatedCost(cost);
        costEstimate.setNumPartitions(numPartitions);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
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
            double size = partStats.rowCount();
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
