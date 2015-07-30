package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.store.access.StoreCostResult;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.store.access.conglomerate.GenericController;
import com.splicemachine.derby.impl.sql.compile.SimpleCostEstimate;
import com.splicemachine.derby.impl.stats.FakedPartitionStatistics;
import com.splicemachine.derby.impl.stats.OverheadManagedTableStatistics;
import com.splicemachine.derby.impl.stats.StatisticsStorage;
import com.splicemachine.derby.impl.stats.StatsConstants;
import com.splicemachine.derby.impl.store.access.base.OpenSpliceConglomerate;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.TableStatistics;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * A Cost Controller which uses underlying Statistics information to estimate the cost of a scan.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public class StatsStoreCostController extends GenericController implements StoreCostController {
    private static Logger LOG = Logger.getLogger(StatsStoreCostController.class);
    protected OverheadManagedTableStatistics conglomerateStatistics;
    protected OpenSpliceConglomerate baseConglomerate;

    public StatsStoreCostController(OpenSpliceConglomerate baseConglomerate) throws StandardException {
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"init baseConglomerate=%s",baseConglomerate.getContainerID());
        this.baseConglomerate = baseConglomerate;
        BaseSpliceTransaction bst = (BaseSpliceTransaction)baseConglomerate.getTransaction();
        TxnView txn = bst.getActiveStateTxn();
        long conglomId = baseConglomerate.getConglomerate().getContainerid();
        try {
            this.conglomerateStatistics = StatisticsStorage.getPartitionStore().getStatistics(txn, conglomId);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG,"init conglomerateStatistics=%s",conglomerateStatistics);
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e);
        }
    }
    @Override
    public double rowCount(){
        return conglomerateStatistics.rowCount();
    }

    @Override
    public double nonNullCount(int columnNumber){
        ColumnStatistics<Object> statistics=conglomerateStatistics.columnStatistics(columnNumber);
        if(statistics!=null)
            return statistics.nonNullCount();
        return rowCount();
    }

    @Override public void close() throws StandardException {  }

    @Override
    public long getEstimatedRowCount() throws StandardException {
        return conglomerateStatistics.rowCount();
    }

    @Override
    public void getFetchFromRowLocationCost(BitSet validColumns,
                                            int access_type,
                                            CostEstimate cost) throws StandardException {
        /*
         * This is useful when estimating the cost of an index lookup. We approximate it by using
         * Remote Latency
         */
        double columnSizeFactor = columnSizeFactor(conglomerateStatistics,
                ((SpliceConglomerate)baseConglomerate.getConglomerate()).getFormat_ids().length,
                validColumns);
        double scale = conglomerateStatistics.remoteReadLatency()*columnSizeFactor;
        long bytes = (long)scale*conglomerateStatistics.avgRowWidth();
        cost.setRemoteCost(cost.remoteCost()+cost.rowCount()*scale);
        cost.setEstimatedHeapSize(cost.getEstimatedHeapSize()+bytes);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFetchFromRowLocationCost={columnSizeFactor=%f, scale=%f, bytes=%d " +
                    "estimatedCost=%d, estimatedHeapSize=%d, estimatedRowCount=%d",columnSizeFactor,scale,bytes,cost.getEstimatedCost(),
            cost.getEstimatedHeapSize(), cost.getEstimatedRowCount());
    }


    @Override
    public void getFetchFromFullKeyCost(BitSet validColumns, int access_type, CostEstimate cost) throws StandardException {
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
        double columnSizeFactor = columnSizeFactor(conglomerateStatistics,
                ((SpliceConglomerate)baseConglomerate.getConglomerate()).getFormat_ids().length,
                validColumns);
        double scale = conglomerateStatistics.remoteReadLatency()*columnSizeFactor;
        long bytes = (long)scale*conglomerateStatistics.avgRowWidth();
        cost.setRemoteCost(scale);
        cost.setLocalCost(conglomerateStatistics.localReadLatency());
        cost.setEstimatedHeapSize(bytes);
        cost.setNumPartitions(1);
        cost.setEstimatedRowCount(1l);
        cost.setOpenCost(conglomerateStatistics.openScannerLatency());
        cost.setCloseCost(conglomerateStatistics.closeScannerLatency());
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFetchFromFullKeyCost={columnSizeFactor=%f, scale=%f, bytes=%d " +
                            "coset=%s",columnSizeFactor,scale,bytes,cost);



    }

    @Override
    public void getScanCost(long row_count,
                            boolean forUpdate,
                            BitSet scanColumnList,
                            DataValueDescriptor[] template,
                            List<DataValueDescriptor> probeValues,
                            DataValueDescriptor[] startKeyValue,
                            int startSearchOperator,
                            DataValueDescriptor[] stopKeyValue,
                            int stopSearchOperator,
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
                ((SpliceConglomerate)baseConglomerate.getConglomerate()).getFormat_ids().length,
                scanColumnList,
                probeValues,startKeyValue,startSearchOperator,
                stopKeyValue,stopSearchOperator,baseConglomerate.getColumnOrdering(),(CostEstimate)cost_result);
        List<? extends PartitionStatistics> partStats=conglomerateStatistics.partitionStatistics();
        if(partStats==null||partStats.size()<=0 ||partStats.get(0) instanceof FakedPartitionStatistics){
            ((CostEstimate)cost_result).setIsRealCost(false);
        }

        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getScanCost costEstimate=%s",cost_result);

        if(cost_result instanceof SimpleCostEstimate){
            ((SimpleCostEstimate)cost_result).setStoreCost(this);
        }
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

    @Override
    public double cardinalityFraction(int columnNumber){
        ColumnStatistics<DataValueDescriptor> colStats=getColumnStats(columnNumber);
        if(colStats!=null){
            double nullFraction=colStats.nullFraction();
            long c=colStats.cardinality();

            return c>0?(1-nullFraction)/c:0d;
        }
        /*
         * If we can't find any statistics for this column, then use a fallback number--arbitrary
         * numbers are better than no numbers at all (although that is somewhat debatable in practice)
         */
        return StatsConstants.fallbackCardinalityFraction;
    }

    protected ColumnStatistics<DataValueDescriptor> getColumnStats(int columnNumber){
        return conglomerateStatistics.columnStatistics(columnNumber);
    }

    protected double nullSelectivityFraction(TableStatistics stats,int columnNumber){
        List<? extends PartitionStatistics> partStats = stats.partitionStatistics();
        long nullCount = 0l;
        int missingStatsCount = partStats.size();
        for(PartitionStatistics pStats:partStats){
            ColumnStatistics<DataValueDescriptor> cStats=pStats.columnStatistics(columnNumber);
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
        List<? extends PartitionStatistics> partStats = stats.partitionStatistics();
        long rowCount = 0l;
        int missingStatsCount = partStats.size();
        for(PartitionStatistics pStats:partStats){
            ColumnStatistics<DataValueDescriptor> cStats = pStats.columnStatistics(columnNumber);
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
    protected void estimateCost(OverheadManagedTableStatistics stats,
                                int totalColumns, //the total number of columns in the store
                                BitSet scanColumnList, //a list of the output columns, or null if fetch all
                                List<DataValueDescriptor> probeValues,
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
        List<? extends PartitionStatistics> partitionStatistics = stats.partitionStatistics();
        boolean includeStop = stopSearchOperator == ScanController.GT;
        boolean includeStart = startSearchOperator != ScanController.GT;
        long numRows = 0l;
        long bytes = 0l;
        double localCost = 0d;
        double remoteCost = stats.openScannerLatency()+stats.closeScannerLatency(); //always the cost to open a scanner
        int numPartitions = 0;
        for(PartitionStatistics pStats:partitionStatistics){
            long partRc = 0l;
            if(probeValues!=null){
                /*
                 * We have probe values, which means that the first entry in the startKeyValue and the stopKeyValue
                 * are populated with an entry, so we loop through here, and if there are rows in this partition
                 * for any of the probe values, then we add a partition in. Otherwise, we do not
                 */
                for(DataValueDescriptor probeValue:probeValues){
                    startKeyValue[0] = probeValue;
                    stopKeyValue[0] = probeValue;
                    partRc+=partitionColumnSelectivity(pStats,startKeyValue,includeStart,stopKeyValue,includeStop,keyMap);
                }
            }else
                partRc=partitionColumnSelectivity(pStats,startKeyValue,includeStart,stopKeyValue,includeStop,keyMap);

            if(partRc>0){
                numPartitions++;
                numRows+=partRc;

                localCost+=pStats.localReadLatency()*partRc;
                double colSizeAdjustment=columnSizeFactor(totalColumns,scanColumnList,pStats);
                double remoteLatency = pStats.remoteReadLatency();
                double size = partRc*colSizeAdjustment;
                remoteCost+=remoteLatency*size;
                bytes+=(colSizeAdjustment*pStats.avgRowWidth())*partRc;
            }
        }

        costEstimate.setEstimatedRowCount(numRows);
        costEstimate.setLocalCost(localCost);
        costEstimate.setRemoteCost(remoteCost);
        //we always touch at least 1 partition
        costEstimate.setNumPartitions(numPartitions>0?numPartitions:1);
        costEstimate.setEstimatedHeapSize(bytes);
        costEstimate.setOpenCost(stats.openScannerLatency());
        costEstimate.setCloseCost(stats.closeScannerLatency());
    }

    private long partitionColumnSelectivity(PartitionStatistics pStats,DataValueDescriptor[] startKeyValue,boolean includeStart,DataValueDescriptor[] stopKeyValue,boolean includeStop,int[] keyMap){
        double selectivity = 1.0d;
        if(startKeyValue==null && stopKeyValue==null){
           return pStats.rowCount();
        }else{
            /*
             * DB-3517. It appears that sometimes the start and stop key value array lengths
             * can differ while not being null. This is weird, and we have to decide what to do.
             * We could A) stop at the min, in which case we only use the keys which are shared in our
             * estimate (bad), or B) treat running off the end of the array as "no bound" on the selectivity
             * estimate. This essentially forces a >= on a start key, or a <= on a stop key.
             */
            int size;
            int stopLen;
            int startLen;
            if(startKeyValue!=null){
                startLen = startKeyValue.length;
                if(stopKeyValue!=null){
                    size=Math.max(startKeyValue.length,stopKeyValue.length);
                    stopLen = stopKeyValue.length;
                }else{
                    size=startKeyValue.length;
                    stopLen = 0;
                }
            }else{
                size=stopLen=stopKeyValue.length;
                startLen = 0;
            }

            for(int i=0;i<size;i++){
                DataValueDescriptor start=startKeyValue!=null&&size<startLen?startKeyValue[i]:null;
                DataValueDescriptor stop=stopKeyValue!=null &&size<stopLen?stopKeyValue[i]:null;
                ColumnStatistics<DataValueDescriptor> cStats=pStats.columnStatistics(keyMap[i]);
                if(cStats!=null){
                    double rc=cStats.getDistribution().rangeSelectivity(start,stop,includeStart,includeStop);
                    selectivity*=rc/cStats.nonNullCount();
                }
            }
            return (long)Math.ceil(selectivity*pStats.rowCount());
        }
    }

    protected double columnSizeFactor(TableStatistics tableStats,int totalColumns,BitSet validColumns){
        //get the average columnSize factor across all regions
        double colFactorSum = 0d;
        List<? extends PartitionStatistics> partStats=tableStats.partitionStatistics();
        if(partStats.size()<=0) return 0d; //no partitions present? huh?

        for(PartitionStatistics pStats: partStats){
            colFactorSum+=columnSizeFactor(totalColumns,validColumns,pStats);
        }
        return colFactorSum/partStats.size();
    }

    private double columnSizeFactor(int totalColumns,BitSet scanColumnList,PartitionStatistics partStats){
        /*
         * Now that we have a base cost, we want to scale that cost down if we
         * are returning fewer columns than the total. To do this, we first
         * compute the average column size for each of the columns that we are interested
         * in (the scanColumnList), and divide it by the average row width. If
         * scanColumnList==null, we assume all columns are interesting, and so we
         * do not scale it.
         *
         * We do have to deal with a situation where there are no statistics for the given
         * column of interest. In this case, we make a guess, where we take the "adjustedRowWidth"(
         * the width of the row - sum(width(column)| all measured columns),and divide it by the
         * number of missing columns to generate a rough estimate.
         */
        int columnSize = 0;
        int adjAvgRowWidth = getAdjustedRowSize(partStats);
        double unknownColumnWidth = -1;
        if(adjAvgRowWidth==0){
            assert partStats.rowCount()==0: "No row width exists, but there is a positive row count!";
            return 0d;
        }
        if(scanColumnList!=null && scanColumnList.cardinality()>0 && scanColumnList.cardinality()!=totalColumns){
            for(int i=scanColumnList.nextSetBit(0);i>=0;i=scanColumnList.nextSetBit(i+1)){
                ColumnStatistics<DataValueDescriptor> cStats = partStats.columnStatistics(i);
                if(cStats!=null)
                    columnSize+=cStats.avgColumnWidth();
                else{
                    /*
                     * There are no statistics for this column, so use the average column width
                     */
                    if(unknownColumnWidth<0){
                        unknownColumnWidth = getUnknownColumnWidth(partStats,totalColumns);
                    }
                    columnSize+=unknownColumnWidth;
                }
            }
        }else {
            columnSize = adjAvgRowWidth; //we are fetching everything, so this ratio is 1:1
        }
        return ((double)columnSize)/adjAvgRowWidth;
    }

    private double getUnknownColumnWidth(PartitionStatistics pStats,int totalColumnCount){
        List<ColumnStatistics> columnStats=pStats.columnStatistics();
        int avgRowWidth = pStats.avgRowWidth();
        int tcc = totalColumnCount;
        for(ColumnStatistics cStats:columnStats){
            int colWidth=cStats.avgColumnWidth();
            avgRowWidth-=colWidth;
            tcc--;
        }
        if(tcc<=0){
            /*
             * This should never happen, because we should guard against it. Still, we want to be careful
             * and not end up with a divide-by-0 goofiness. In this scenario, we just return an "Average"
             */
            return pStats.avgRowWidth()/totalColumnCount;
        } else if(avgRowWidth<0){
            /*
             * This is another weird situation that PROBABLY should never happen, where we somehow
             * mis-compute the average width of a row relative to the average width of all the columns, and we
             * end up with a case where the sum of the average column widths is > the average row width as
             * computed overall.
             *
             * This shouldn't happen ever, because we include SI columns and other overhead when we compute
             * the average row width, but in the spirit of extra safety, we include this check here. When
             * this happens, we just delegate to the average
             */
            return pStats.avgRowWidth()/totalColumnCount;
        }else
            return ((double)avgRowWidth)/totalColumnCount;
    }

    private int getAdjustedRowSize(PartitionStatistics pStats){

        return pStats.avgRowWidth();
    }
}
