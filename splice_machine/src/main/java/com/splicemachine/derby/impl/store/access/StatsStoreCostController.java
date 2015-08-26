package com.splicemachine.derby.impl.store.access;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.store.access.conglomerate.GenericController;
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

    /**
     *
     * Returns the rowCount from the conglomerate statistics
     *
     * @return double rowCount
     */
    @Override
    public double rowCount(){
        return conglomerateStatistics.rowCount();
    }

    /**
     *
     * Returns the nonNullCount from the column statistics if available or returns the total row count
     *
     * @return double rowCount
     */

    @Override
    public double nonNullCount(int columnNumber){
        ColumnStatistics<Object> statistics=conglomerateStatistics.columnStatistics(columnNumber);
        if(statistics!=null)
            return statistics.nonNullCount();
        return rowCount();
    }

    @Override public void close() throws StandardException {  }

    /**
     * Returns the rowCount from the statistics as a long
     *
     * @return
     * @throws StandardException
     */
    @Override
    public long getEstimatedRowCount() throws StandardException {
        return conglomerateStatistics.rowCount();
    }

    @Override
    public double conglomerateColumnSizeFactor(BitSet validColumns) {
        return columnSizeFactor(conglomerateStatistics,
                ((SpliceConglomerate)baseConglomerate.getConglomerate()).getFormat_ids().length,
                validColumns);
    }

    @Override
    public double baseTableColumnSizeFactor(BitSet validColumns) {
        return conglomerateColumnSizeFactor(validColumns);
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
        cost.setRemoteCost(conglomerateStatistics.remoteReadLatency()*columnSizeFactor*conglomerateStatistics.avgRowWidth());
        cost.setLocalCost(conglomerateStatistics.localReadLatency());
        cost.setEstimatedHeapSize((long) columnSizeFactor*conglomerateStatistics.avgRowWidth());
        cost.setNumPartitions(1);
        cost.setEstimatedRowCount(1l);
        cost.setOpenCost(conglomerateStatistics.openScannerLatency());
        cost.setCloseCost(conglomerateStatistics.closeScannerLatency());
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFetchFromFullKeyCost={columnSizeFactor=%f, cost=%s" +
                            "cost=%s",columnSizeFactor,cost);

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
    public long cardinality(int columnNumber){
        ColumnStatistics<DataValueDescriptor> colStats=getColumnStats(columnNumber);
        if(colStats!=null)
            return colStats.cardinality();
        return 0;
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
            return StatsConstants.fallbackNullFraction;
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
        if (stats.rowCount() == 0)
            return 0.0d;
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
        if (stats.rowCount() == 0)
            return 0.0d;
        rc+=rowCount;
        double returnValue =rc/stats.rowCount();
        assert returnValue >= 0.0d && returnValue <= 1.0d:"Incorrect Selectivity Fraction Returned from Statistics: Critical Error (DB-3729)";
        return returnValue;
    }

    protected double columnSizeFactor(TableStatistics tableStats,int totalColumns,BitSet validColumns){
        //get the average columnSize factor across all regions
        double colFactorSum = 0d;
        List<? extends PartitionStatistics> partStats=tableStats.partitionStatistics();
        if(partStats.size()<=0) return 1d; //no partitions present? huh?

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

    @Override
    public long getConglomerateAvgRowWidth() {
        return conglomerateStatistics.avgRowWidth();
    }

    @Override
    public long getBaseTableAvgRowWidth() {
        return conglomerateStatistics.avgRowWidth();
    }

    @Override
    public double getLocalLatency() {
        return conglomerateStatistics.localReadLatency();
    }

    @Override
    public double getRemoteLatency() {
        return conglomerateStatistics.remoteReadLatency();
    }

    @Override
    public int getNumPartitions() {
        return conglomerateStatistics.partitionStatistics().size();
    }
}
