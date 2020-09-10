/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */
package com.splicemachine.derby.impl.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.context.ContextService;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.*;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.catalog.SYSTABLESTATISTICSRowFactory;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Function;
import splice.com.google.common.collect.Lists;
import splice.com.google.common.collect.Maps;
import java.io.IOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class StoreCostControllerImpl implements StoreCostController {

    private static Logger LOG = Logger.getLogger(StoreCostControllerImpl.class);

    private static final Function<? super Partition,? extends String> partitionNameTransform = new Function<Partition, String>(){
        @Override public String apply(Partition hRegionInfo) {
            assert hRegionInfo != null : "regionInfo cannot be null!";
            return hRegionInfo.getName();
        }
    };

    private static final Function<PartitionStatisticsDescriptor,String> partitionStatisticsTransform = new Function<PartitionStatisticsDescriptor, String>(){
        @Override public String apply(PartitionStatisticsDescriptor desc){
            assert desc!=null: "Descriptor cannot be null!";
            return desc.getPartitionId();
        }
    };

    private final double openLatency;
    private final double closeLatency;
    private final double fallbackNullFraction;
    private final double extraQualifierMultiplier;
    private int missingPartitions;
    private final TableStatistics tableStatistics;
    private final boolean useRealTableStatistics;
    private final double fallbackLocalLatency;
    private final double fallbackRemoteLatencyRatio;
    private final ExecRow baseTableRow;
    private final int conglomerateColumns;
    private boolean noStats;
    private boolean isSampleStats;
    private double sampleFraction;
    private boolean isMergedStats;


    public StoreCostControllerImpl(TableDescriptor td, ConglomerateDescriptor conglomerateDescriptor, List<PartitionStatisticsDescriptor> partitionStatistics, long defaultRowCount) throws StandardException {
        SConfiguration config = EngineDriver.driver().getConfiguration();
        openLatency = config.getFallbackOpencloseLatency();
        closeLatency = config.getFallbackOpencloseLatency();
        fallbackNullFraction = config.getFallbackNullFraction();
        extraQualifierMultiplier = config.getOptimizerExtraQualifierMultiplier();
        fallbackLocalLatency =config.getFallbackLocalLatency();
        fallbackRemoteLatencyRatio =config.getFallbackRemoteLatencyRatio();
        String tableId = Long.toString(td.getBaseConglomerateDescriptor().getConglomerateNumber());
        baseTableRow = td.getEmptyExecRow();
        if (conglomerateDescriptor.getIndexDescriptor() != null &&
            conglomerateDescriptor.getIndexDescriptor().getIndexDescriptor() != null)
        {
            if (conglomerateDescriptor.getIndexDescriptor().isPrimaryKey())
                conglomerateColumns = td.getNumberOfColumns();
            else
                conglomerateColumns = conglomerateDescriptor.getIndexDescriptor().numberOfOrderedColumns();
        } else {
            conglomerateColumns = (conglomerateDescriptor.getColumnNames() == null) ? 2 : conglomerateDescriptor.getColumnNames().length;
        }
        byte[] table = Bytes.toBytes(tableId);

        isSampleStats = false;
        sampleFraction = 0.0d;
        if (!partitionStatistics.isEmpty()) {
            int statsType = partitionStatistics.get(0).getStatsType();
            isSampleStats = statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_NONMERGED_STATS || statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS;
            isMergedStats = statsType == SYSTABLESTATISTICSRowFactory.REGULAR_MERGED_STATS || statsType == SYSTABLESTATISTICSRowFactory.SAMPLE_MERGED_STATS || statsType == SYSTABLESTATISTICSRowFactory.FAKE_MERGED_STATS;
            if (isSampleStats)
                sampleFraction = partitionStatistics.get(0).getSampleFraction();
        }

        List<Partition> partitions = new ArrayList<>();
        List<PartitionStatistics> partitionStats;
        if (td.getTableType() != TableDescriptor.EXTERNAL_TYPE && !isMergedStats) {
            getPartitions(table, partitions, false);
            assert partitions != null && !partitions.isEmpty() : "No Partitions returned";
            List<String> partitionNames = Lists.transform(partitions, partitionNameTransform);
            LanguageConnectionContext lcc = (LanguageConnectionContext) ContextService.getContext(LanguageConnectionContext.CONTEXT_ID);
            Map<String, PartitionStatisticsDescriptor> partitionMap = Maps.uniqueIndex(partitionStatistics, partitionStatisticsTransform);
            if (partitions.size() < partitionStatistics.size()) {
                // reload if partition cache contains outdated data for this table
                partitions.clear();
                getPartitions(table, partitions, true);
            }
            partitionStats = new ArrayList<>(partitions.size());
            PartitionStatisticsDescriptor tStats;


            for (String partitionName : partitionNames) {
                tStats = partitionMap.get(partitionName);
                if (tStats == null) {
                    missingPartitions++;
                    continue; //skip missing partitions entirely
                }
                partitionStats.add(new PartitionStatisticsImpl(tStats, fallbackNullFraction,extraQualifierMultiplier));
            }
        } else {
            partitionStats = new ArrayList<>(partitionStatistics.size());
            for (PartitionStatisticsDescriptor tStats : partitionStatistics) {
                partitionStats.add(new PartitionStatisticsImpl(tStats, fallbackNullFraction,extraQualifierMultiplier));
            }
        }

        /*
         * We cannot have *zero* completely populated items unless we have no column statistics, but in that case
         * we have no table information either, so just return an empty list and let the caller figure out
         * what to do
         */
        if (partitionStats.isEmpty()) {
            missingPartitions = 0;
            noStats = true;
            if (td.getTableType() != TableDescriptor.EXTERNAL_TYPE)
                tableStatistics = RegionLoadStatistics.getTableStatistics(tableId, partitions,fallbackNullFraction,extraQualifierMultiplier, defaultRowCount);
            else {
                try {
                    FileInfo fileInfo = ImportUtils.getImportFileInfo(td.getLocation());
                    long rowCount = fileInfo !=null?fileInfo.size()/100:(long) VTICosting.defaultEstimatedRowCount;
                    long heapSize = fileInfo !=null?fileInfo.size():(long) VTICosting.defaultEstimatedRowCount*100;
                    if (defaultRowCount > 0) {
                        rowCount = defaultRowCount;
                        heapSize = rowCount * 100;
                    }
                    FakePartitionStatisticsImpl impl = new FakePartitionStatisticsImpl(
                            tableId,tableId,rowCount,heapSize,fallbackNullFraction,extraQualifierMultiplier);
                    partitionStats.add(impl);
                    tableStatistics = new TableStatisticsImpl(tableId,partitionStats,fallbackNullFraction,extraQualifierMultiplier);
                } catch (Exception e) {
                    throw StandardException.plainWrapException(e);
                }
            }
            useRealTableStatistics = false;
        } else {
            tableStatistics = new TableStatisticsImpl(tableId, partitionStats,fallbackNullFraction,extraQualifierMultiplier);
            useRealTableStatistics = true;
        }
    }

    @Override
    public void close() throws StandardException {

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
        double columnSizeFactor = conglomerateColumnSizeFactor(validColumns);

        cost.setRemoteCost(getRemoteLatency()*columnSizeFactor*tableStatistics.avgRowWidth());
        cost.setLocalCost(fallbackLocalLatency);
        cost.setEstimatedHeapSize((long) columnSizeFactor*tableStatistics.avgRowWidth());
        cost.setNumPartitions(1);
        cost.setRemoteCostPerPartition(cost.remoteCost());
        cost.setLocalCostPerPartition(cost.localCost());
        cost.setEstimatedRowCount(1l);
        cost.setOpenCost(openLatency);
        cost.setCloseCost(closeLatency);
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"getFetchFromFullKeyCost={columnSizeFactor=%f, cost=%s" +
                    "cost=%s",columnSizeFactor,cost);


    }

    @Override
    public RowLocation newRowLocationTemplate() throws StandardException {
        return null;
    }

    @Override
    public double getSelectivity(int columnNumber, DataValueDescriptor start, boolean includeStart, DataValueDescriptor stop, boolean includeStop, boolean useExtrapolation) {
        return tableStatistics.rangeSelectivity(start,stop,includeStart,includeStop,columnNumber-1, useExtrapolation);
    }

    @Override
    @SuppressFBWarnings(value = "ICAST_IDIV_CAST_TO_DOUBLE", justification = "DB-9844")
    public double rowCount() {
        double rowCnt = tableStatistics.rowCount();
        if (isSampleStats)
            rowCnt = rowCnt/sampleFraction;
        if (missingPartitions > 0) {
            assert tableStatistics.numPartitions() > 0: "Number of partitions cannot be 0 ";
            return rowCnt + rowCnt * ((double)missingPartitions / tableStatistics.numPartitions());
        }
        else
            return rowCnt;
    }

    @Override
    @SuppressFBWarnings(value = "ICAST_IDIV_CAST_TO_DOUBLE", justification = "DB-9844")
    public double nonNullCount(int columnNumber) {
        double notNullCount = tableStatistics.notNullCount(columnNumber - 1);
        if (isSampleStats)
            notNullCount = notNullCount/sampleFraction;
        if (missingPartitions > 0) {
            assert tableStatistics.numPartitions() > 0: "Number of partitions cannot be 0";
            return notNullCount + notNullCount * ((double)missingPartitions / tableStatistics.numPartitions());
        } else
            return notNullCount;
    }

    @Override
    public double nullSelectivity(int columnNumber) {
        long rowCount = tableStatistics.rowCount();
        long nonNullCount = tableStatistics.notNullCount(columnNumber-1);
        // If a column has null values for all rows, set its null selectivity to be slightly less than 1 to prevent
        // nonnull selectivity from being 0.
        if (rowCount == 0)
            return 0.0d;
        else if (nonNullCount == 0)
            return (double)(rowCount - 1) / (double)rowCount;
        else
            return (double)(rowCount - nonNullCount) / (double)rowCount;


    }

    @Override
    public long cardinality(int columnNumber) {
        if (missingPartitions > 0)
            assert tableStatistics.numPartitions() > 0: "Number of partitions cannot be 0";
        /** Even when there are partitions with missing stats, we can still assume that these partitions
         *  do not contribute more unique values, thus have the same cardinality as the rest partitions
         */
        /** Currently, we assume naively that with sample stats, we also see all the distinct values, so there is no
         * need to scale based on sample fraction.
         * Possible enhancement is to take into consideration the property of columns (e.g., is it distinct, is it boolean)
         * to determine the extrapolation logic.
         */
        return tableStatistics.cardinality(columnNumber-1);
    }

    @Override
    public long getConglomerateAvgRowWidth() {
        assert baseTableRow.nColumns() > 0: "Number of base table columns cannot be 0";
        return
                (long) (((double) tableStatistics.avgRowWidth())
                        *
                        ( (double) conglomerateColumns / ((double) baseTableRow.nColumns())));
    }

    @Override
    public long getBaseTableAvgRowWidth() {
        return noStats || tableStatistics.avgRowWidth() ==0 ?baseTableRow.length():tableStatistics.avgRowWidth();
    }

    @Override
    public double conglomerateColumnSizeFactor(BitSet validColumns) {
        assert conglomerateColumns > 0: "Conglomerate Columns cannot be 0";
        return ( (double) validColumns.cardinality())/ ((double) conglomerateColumns);
    }

    @Override
    public double baseTableColumnSizeFactor(BitSet validColumns) {
        assert baseTableRow.nColumns() > 0: "Base Table N Columns cannot be 0";
        return ( (double) validColumns.cardinality())/ ((double) baseTableRow.nColumns());
    }

    @Override
    public double getLocalLatency() {
        return fallbackLocalLatency;
    }

    @Override
    public double getRemoteLatency() {
        return fallbackLocalLatency*fallbackRemoteLatencyRatio;
    }

    @Override
    public double getOpenLatency() {
        return openLatency;
    }

    @Override
    public double getCloseLatency() {
        return closeLatency;
    }

    @Override
    public int getNumPartitions() {
        return missingPartitions+tableStatistics.numPartitions();
    }


    @Override
    public double baseRowCount() {
        return rowCount();
    }

    @Override
    public DataValueDescriptor minValue(int columnNumber) {
        return tableStatistics.minValue(columnNumber-1);
    }

    @Override
    public DataValueDescriptor maxValue(int columnNumber) {
        return tableStatistics.maxValue(columnNumber-1);
    }

    @Override
    public long getEstimatedRowCount() throws StandardException {
        return (long)rowCount();
    }

    public static Txn getTxn(TxnView wrapperTxn) throws ExecutionException {
        try {
            return SIDriver.driver().lifecycleManager().beginChildTransaction(wrapperTxn, Txn.IsolationLevel.READ_UNCOMMITTED,null);
        } catch (IOException e) {
            throw new ExecutionException(e);
        }
    }

    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_EXCEPTION", justification = "DB-9844")
    public static int getPartitions(byte[] table, List<Partition> partitions, boolean refresh) throws StandardException {
        try (Partition root = SIDriver.driver().getTableFactory().getTable(table)) {
            partitions.addAll(root.subPartitions(refresh));
            return partitions.size();
        } catch (Exception ioe) {
            throw StandardException.plainWrapException(ioe);
        }
    }

    public static int getPartitions(String table, List<Partition> partitions) throws StandardException {
        return getPartitions(table,partitions,false);
    }

    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH_EXCEPTION", justification = "DB-9844")
    public static int getPartitions(String table, List<Partition> partitions, boolean refresh) throws StandardException {
        try (Partition root = SIDriver.driver().getTableFactory().getTable(table)) {
            partitions.addAll(root.subPartitions(refresh));
            return partitions.size();
        } catch (Exception ioe) {
            throw StandardException.plainWrapException(ioe);
        }
    }

    public double getSelectivityExcludingValueIfSkewed(int columnNumber, DataValueDescriptor value) {
        return tableStatistics.selectivityExcludingValueIfSkewed(value, columnNumber-1);
    }

    @Override
    public boolean useRealTableStatistics() {
        return useRealTableStatistics;
    }

    @Override
    public boolean useRealColumnStatistics(int columnNumber) {
        if (!useRealTableStatistics || columnNumber <= 0)  // rowid column number == 0
            return false;
        PartitionStatistics ps = tableStatistics.getPartitionStatistics().get(0);
        return ps.getColumnStatistics(columnNumber - 1) != null;
    }
}
