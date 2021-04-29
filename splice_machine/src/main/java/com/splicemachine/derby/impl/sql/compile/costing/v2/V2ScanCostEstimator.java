/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.impl.sql.compile.costing.v2;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.BitSet;
import java.util.HashSet;

public class V2ScanCostEstimator extends AbstractScanCostEstimator {
    private static final double SCAN_OPEN_LATENCY   = 700;    // 700 microseconds
    private static final double SCAN_CLOSE_LATENCY  = 700;    // 700 microseconds
    private static final double LOCAL_LATENCY       = 3;      // 3   microseconds (per 100 bytes)
    private static final double OLAP_START_OVERHEAD = 140000; // 140 milliseconds

    /**
     * <pre>
     *
     *     Selectivity is computed at 3 levels.
     *
     *     BASE -> Qualifiers on the row keys (start/stop Qualifiers, Multiprobe)
     *     FILTER_BASE -> Qualifiers applied after the scan but before the index lookup.
     *     FILTER_PROJECTION -> Qualifers and Predicates applied after any potential lookup, usually performed on the Projection Node in the scan.
     *
     *
     * </pre>
     */
    public V2ScanCostEstimator(Optimizable baseTable,
                               ConglomerateDescriptor cd,
                               StoreCostController scc,
                               CostEstimate scanCost,
                               ResultColumnList resultColumns,
                               DataValueDescriptor[] scanRowTemplate,
                               BitSet baseColumnsInScan,
                               BitSet baseColumnsInLookup,
                               int indexLookupBatchRowCount,
                               int indexLookupConcurrentBatchesCount,
                               boolean forUpdate,
                               boolean isOlap, HashSet<Integer> usedNoStatsColumnIds) throws StandardException {
        super(baseTable, cd, scc, scanCost, resultColumns, scanRowTemplate, baseColumnsInScan, baseColumnsInLookup,
              indexLookupBatchRowCount, indexLookupConcurrentBatchesCount, forUpdate, isOlap, usedNoStatsColumnIds);
    }

    /**
     *
     * Add Predicate and keep track of the selectivity
     *
     * @param p
     * @throws StandardException
     */
    public void addPredicate(Predicate p, double defaultSelectivityFactor) throws StandardException{
        if (p.isMultiProbeQualifier(indexColumns)) {// MultiProbeQualifier against keys (BASE)
            addSelectivity(new InListSelectivity(scc, p, isIndexOnExpression ? indexColumns : null, QualifierPhase.BASE, defaultSelectivityFactor), SCAN);
            collectNoStatsColumnsFromInListPred(p);
        } else if (p.isInQualifier(baseColumnsInScan)) { // In Qualifier in Base Table (FILTER_PROJECTION) // This is not as expected, needs more research.
            addSelectivity(new InListSelectivity(scc, p, null, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor), SCAN);
            accumulateExprEvalCost(p);
            collectNoStatsColumnsFromInListPred(p);
        }
        else if (p.isInQualifier(baseColumnsInLookup)) { // In Qualifier against looked up columns (FILTER_PROJECTION)
            addSelectivity(new InListSelectivity(scc, p, null, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor), TOP);
            accumulateExprEvalCost(p);
            collectNoStatsColumnsFromInListPred(p);
        }
        else if ( (p.isStartKey() || p.isStopKey()) && scanPredicatePossible) { // Range Qualifier on Start/Stop Keys (BASE)
            performQualifierSelectivity(p, QualifierPhase.BASE, isIndexOnExpression, defaultSelectivityFactor, SCAN);
            if (!p.isStartKey() || !p.isStopKey()) // Only allows = to further restrict BASE scan numbers
                scanPredicatePossible = false;
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else if (p.isQualifier()) { // Qualifier in Base Table (FILTER_BASE)
            performQualifierSelectivity(p, QualifierPhase.FILTER_BASE, isIndexOnExpression, defaultSelectivityFactor, SCAN);
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else if (PredicateList.isQualifier(p,baseTable,cd,false)) { // Qualifier on Base Table After Index Lookup (FILTER_PROJECTION)
            performQualifierSelectivity(p, QualifierPhase.FILTER_PROJECTION, isIndexOnExpression, defaultSelectivityFactor, TOP);
            accumulateExprEvalCost(p);
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else { // Project Restrict Selectivity Filter
            addSelectivity(new DefaultPredicateSelectivity(p, baseTable, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor), TOP);
            accumulateExprEvalCost(p);
        }
    }

    /**
     *
     * Compute the Base Scan Cost by utilizing the passed in StoreCostController
     *
     * @throws StandardException
     */

    public void generateCost(long numFirstIndexColumnProbes) throws StandardException {

        double baseTableSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder, QualifierPhase.BASE);
        double filterBaseTableSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder,QualifierPhase.BASE,QualifierPhase.FILTER_BASE);
        double projectionSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder,QualifierPhase.FILTER_PROJECTION);
        double totalSelectivity = computeTotalSelectivity(scanSelectivityHolder, topSelectivityHolder);

        assert filterBaseTableSelectivity >= 0 && filterBaseTableSelectivity <= 1.0:"filterBaseTableSelectivity Out of Bounds -> " + filterBaseTableSelectivity;
        assert baseTableSelectivity >= 0 && baseTableSelectivity <= 1.0:"baseTableSelectivity Out of Bounds -> " + baseTableSelectivity;
        assert projectionSelectivity >= 0 && projectionSelectivity <= 1.0:"projectionSelectivity Out of Bounds -> " + projectionSelectivity;
        assert totalSelectivity >= 0 && totalSelectivity <= 1.0:"totalSelectivity Out of Bounds -> " + totalSelectivity;

        // Total Row Count from the Base Conglomerate
        double totalRowCount = scc.baseRowCount();
        assert totalRowCount >= 0 : "totalRowCount cannot be negative -> " + totalRowCount;
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        scanCost.setEstimatedRowCount(Math.round(totalRowCount*totalSelectivity));

        int numCols = getTotalNumberOfBaseColumnsInvolved();
        if (isIndexOnExpression && numCols == 0) {
            // Scanning a covering expression-based index. No base table columns is scanned if we choose this path,
            // use number of index columns instead. This may over estimate because probably not all index columns
            // are referenced but should be better than using 0. Otherwise, heap size is always 0 and remote cost is
            // a constant value for a covering expression-based index scan.
            numCols = indexDescriptor.getIndexColumnTypes().length;
        }
        double baseTableColumnSizeFactor = scc.baseTableColumnSizeFactor(numCols);
        double baseTableAverageRowWidth = scc.getBaseTableAvgRowWidth();

        // We use the base table so the estimated heap size and remote cost are the same for all conglomerates
        double colSizeFactor = baseTableAverageRowWidth*baseTableColumnSizeFactor;
        assert baseTableAverageRowWidth >= 0 : "baseTableAverageRowWidth cannot be negative -> " + baseTableAverageRowWidth;
        assert baseTableColumnSizeFactor >= 0 : "baseTableColumnSizeFactor cannot be negative -> " + baseTableColumnSizeFactor;

        double openLatency = SCAN_OPEN_LATENCY;
        double closeLatency = SCAN_CLOSE_LATENCY;
        double localLatency = LOCAL_LATENCY;
        double remoteLatency = scc.getRemoteLatency();

        double remoteCost = (openLatency + closeLatency) +
                (numFirstIndexColumnProbes*2)*remoteLatency*(1+colSizeFactor/1024d) +
                totalRowCount*totalSelectivity*remoteLatency*(1+colSizeFactor/1024d); // Per Kb

        assert remoteLatency >= 0 : "remoteLatency cannot be negative -> " + remoteLatency;
        assert remoteCost >= 0 : "remoteCost cannot be negative -> " + remoteCost;
        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        scanCost.setEstimatedHeapSize((long)(totalRowCount*totalSelectivity*colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long)remoteCost);

        int numPartitions = scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1;
        assert numPartitions >= 1 : "invalid number of partitions: " + numPartitions;
        int parallelism = scc.getParallelism() != 0 ? scc.getParallelism() : 1;
        assert parallelism >= 1 : "invalid parallelism: " + parallelism;

        // base Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        assert congAverageWidth >= 0 : "congAverageWidth cannot be negative -> " + congAverageWidth;
        assert numFirstIndexColumnProbes >= 0;

        double baseCost = openLatency + closeLatency;
        baseCost += (numFirstIndexColumnProbes * 2) * localLatency * (1 + congAverageWidth / 100d);
        baseCost += (totalRowCount * baseTableSelectivity * localLatency * (1 + congAverageWidth / 100d));
        if (isOlap) {
            double olapReductionFactor = Math.max(2, Math.min(Math.log(numPartitions), Math.log(parallelism)));
            baseCost = baseCost / olapReductionFactor + OLAP_START_OVERHEAD;
        }

        assert baseCost >= 0 : "baseCost cannot be negative -> " + baseCost;
        scanCost.setFromBaseTableRows(Math.round(filterBaseTableSelectivity * totalRowCount));
        scanCost.setFromBaseTableCost(baseCost);
        // set how many base table rows to scan
        scanCost.setScannedBaseTableRows(Math.round(baseTableSelectivity * totalRowCount));

        // lookup cost
        double lookupCost;
        if (baseColumnsInLookup == null) {
            lookupCost = 0.0d;
            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            scanCost.setIndexLookupRows(-1.0d);
            scanCost.setIndexLookupCost(-1.0d);
        } else {
            double lookupRowsCount = totalRowCount * filterBaseTableSelectivity;
            lookupCost = estimateIndexLookupCost(lookupRowsCount, openLatency, closeLatency);
            scanCost.setIndexLookupRows(Math.round(lookupRowsCount));
            scanCost.setIndexLookupCost(lookupCost + baseCost);
        }
        assert lookupCost >= 0 : "lookupCost cannot be negative -> " + lookupCost;

        // projection cost
        double projectionCost;
        if (projectionSelectivity == 1.0d) {
            projectionCost = 0.0d;
            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            scanCost.setProjectionRows(-1.0d);
            scanCost.setProjectionCost(-1.0d);
        } else {
            projectionCost = totalRowCount * filterBaseTableSelectivity * (localLatency * colSizeFactor*1d/1000d + exprEvalCostPerRow);
            scanCost.setProjectionRows((double) scanCost.getEstimatedRowCount());
            scanCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        }
        assert projectionCost >= 0 : "projectionCost cannot be negative -> " + projectionCost;

        double localCost = baseCost+lookupCost+projectionCost;
        assert localCost >= 0 : "localCost cannot be negative -> " + localCost;
        scanCost.setLocalCost(localCost);
        scanCost.setFirstColumnStats(scc.getFirstColumnStats());
        scanCost.setNumPartitions(numPartitions);
        scanCost.setParallelism(parallelism);
        scanCost.setLocalCostPerParallelTask((baseCost + lookupCost + projectionCost), parallelism);
        scanCost.setRemoteCostPerParallelTask(scanCost.remoteCost(), parallelism);

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("%n" +
                                            "============= generateCost() for table: %s =============%n" +
                                            "Conglomerate:               %s, %n" +
                                            "baseTableSelectivity:       %.18f, %n" +
                                            "filterBaseTableSelectivity: %.18f, %n" +
                                            "projectionSelectivity:      %.18f, %n" +
                                            "totalSelectivity:           %.18f, %n" +
                                            "totalRowCount:              %.1f, %n" +
                                            "heapSize:                   %d, %n" +
                                            "congAverageWidth:           %.1f, %n" +
                                            "fromBaseTableRows:          %.1f, %n" +
                                            "scannedBaseTableRows:       %.1f, %n" +
                                            "fromBaseTableCost:          %.1f, %n" +
                                            "remoteCost:                 %.1f, %n" +
                                            "indexLookupRows:            %.1f, %n" +
                                            "lookupCost:                 %.1f, %n" +
                                            "projectRows:                %.1f, %n" +
                                            "projectCost:                %.1f, %n" +
                                            "localCost:                  %.1f, %n" +
                                            "numPartition:               %d, %n" +
                                            "localCostPerPartition:      %.1f %n" +
                                            "========================================================%n",
                                    baseTable.getBaseTableName(),
                                    baseTable.getCurrentAccessPath().getConglomerateDescriptor().toString(),
                                    baseTableSelectivity,
                                    filterBaseTableSelectivity, projectionSelectivity, totalSelectivity,
                                    scanCost.rowCount(), scanCost.getEstimatedHeapSize(), congAverageWidth,
                                    scanCost.getFromBaseTableRows(), scanCost.getScannedBaseTableRows(),
                                    scanCost.getFromBaseTableCost(), scanCost.getRemoteCost(),
                                    scanCost.getIndexLookupRows(), scanCost.getIndexLookupCost(),
                                    scanCost.getProjectionRows(), scanCost.getProjectionCost(),
                                    scanCost.getLocalCost(), scc.getNumPartitions(), scanCost.getLocalCost()/scc.getNumPartitions()));
        }
    }

    public void generateOneRowCost() throws StandardException {
        // Total Row Count from the Base Conglomerate
        double totalRowCount = 1.0d;
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        scanCost.setEstimatedRowCount(Math.round(totalRowCount));

        int numCols = getTotalNumberOfBaseColumnsInvolved();
        if (isIndexOnExpression && numCols == 0) {
            // Scanning a covering expression-based index. No base table columns is scanned if we choose this path,
            // use number of index columns instead. This may over estimate because probably not all index columns
            // are referenced but should be better than using 0. Otherwise, heap size is always 0 and remote cost is
            // a constant value for a covering expression-based index scan.
            numCols = indexDescriptor.getIndexColumnTypes().length;
        }
        double baseTableColumnSizeFactor = scc.baseTableColumnSizeFactor(numCols);
        double baseTableAverageRowWidth = scc.getBaseTableAvgRowWidth();

        // We use the base table so the estimated heap size and remote cost are the same for all conglomerates
        double colSizeFactor = baseTableAverageRowWidth*baseTableColumnSizeFactor;

        double openLatency = SCAN_OPEN_LATENCY;
        double closeLatency = SCAN_CLOSE_LATENCY;
        double localLatency = LOCAL_LATENCY;
        double remoteLatency = scc.getRemoteLatency();

        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        scanCost.setEstimatedHeapSize((long) (totalRowCount * colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long) (openLatency + closeLatency + totalRowCount * remoteLatency * (1 + colSizeFactor / 100d)));
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = openLatency + closeLatency + (totalRowCount * localLatency * (1 + scc.getConglomerateAvgRowWidth() / 100d));
        if (isOlap) {
            // baseCost should be very small for 1 row and dividing it by reduction factor doesn't make much difference
            baseCost = baseCost + OLAP_START_OVERHEAD;
        }
        scanCost.setFromBaseTableRows(totalRowCount);
        scanCost.setFromBaseTableCost(baseCost);
        scanCost.setScannedBaseTableRows(totalRowCount);
        double lookupCost;
        if (baseColumnsInLookup == null) {
            lookupCost = 0.0d;

            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            scanCost.setIndexLookupRows(-1.0d);
            scanCost.setIndexLookupCost(-1.0d);
        } else {
            lookupCost = estimateIndexLookupCost(totalRowCount, scc.getOpenLatency(), scc.getCloseLatency());
            scanCost.setIndexLookupRows(totalRowCount);
            scanCost.setIndexLookupCost(lookupCost+baseCost);
        }
        double projectionCost = totalRowCount * (scc.getLocalLatency() * colSizeFactor*1d/1000d + exprEvalCostPerRow);
        scanCost.setProjectionRows(scanCost.getEstimatedRowCount());
        scanCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        scanCost.setLocalCost(baseCost+lookupCost+projectionCost);
        scanCost.setFirstColumnStats(scc.getFirstColumnStats());
        scanCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        scanCost.setParallelism(scc.getParallelism() != 0 ? scc.getParallelism() : 1);
        scanCost.setLocalCostPerParallelTask(scanCost.localCost(), scanCost.getParallelism());
        scanCost.setRemoteCostPerParallelTask(scanCost.remoteCost(), scanCost.getParallelism());
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("%n" +
                                            "============= generateOneRowCost() for table: %s =============%n" +
                                            "Conglomerate:               %s, %n" +
                                            "totalRowCount:              %.1f, %n" +
                                            "heapSize:                   %d, %n" +
                                            "congAverageWidth:           %.1f, %n" +
                                            "fromBaseTableRows:          %.1f, %n" +
                                            "scannedBaseTableRows:       %.1f, %n" +
                                            "fromBaseTableCost:          %.1f, %n" +
                                            "remoteCost:                 %.1f, %n" +
                                            "indexLookupRows:            %.1f, %n" +
                                            "lookupCost:                 %.1f, %n" +
                                            "projectRows:                %.1f, %n" +
                                            "projectCost:                %.1f, %n" +
                                            "localCost:                  %.1f, %n" +
                                            "numPartition:               %d, %n" +
                                            "localCostPerPartition:      %.1f %n" +
                                            "========================================================%n",
                                    baseTable.getBaseTableName(),
                                    baseTable.getCurrentAccessPath().getConglomerateDescriptor().toString(),
                                    scanCost.rowCount(), scanCost.getEstimatedHeapSize(), congAverageWidth,
                                    scanCost.getFromBaseTableRows(), scanCost.getScannedBaseTableRows(),
                                    scanCost.getFromBaseTableCost(), scanCost.getRemoteCost(),
                                    scanCost.getIndexLookupRows(), scanCost.getIndexLookupCost(),
                                    scanCost.getProjectionRows(), scanCost.getProjectionCost(),
                                    scanCost.getLocalCost(), scc.getNumPartitions(), scanCost.getLocalCost()/scc.getNumPartitions()));
        }
    }
}
