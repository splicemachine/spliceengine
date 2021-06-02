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

package com.splicemachine.derby.impl.sql.compile.costing;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.BitSet;
import java.util.HashSet;

public class V1ScanCostEstimator extends AbstractScanCostEstimator {
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
    public V1ScanCostEstimator(Optimizable baseTable,
                               ConglomerateDescriptor cd,
                               StoreCostController scc,
                               CostEstimate scanCost,
                               ResultColumnList resultColumns,
                               DataValueDescriptor[] scanRowTemplate,
                               BitSet baseColumnsInScan,
                               BitSet baseColumnsInLookup,
                               boolean forUpdate,
                               HashSet<Integer> usedNoStatsColumnIds) throws StandardException {
        super(baseTable, cd, scc, scanCost, resultColumns, scanRowTemplate, baseColumnsInScan, baseColumnsInLookup, forUpdate, usedNoStatsColumnIds);
    }

    /**
     * {@inheritDoc}
     */
    public void addPredicate(Predicate p, double defaultSelectivityFactor, Optimizer optimizer) throws StandardException{
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
            performQualifierSelectivity(p, QualifierPhase.BASE, isIndexOnExpression, defaultSelectivityFactor, SCAN, optimizer);
            if (!p.isStartKey() || !p.isStopKey()) // Only allows = to further restrict BASE scan numbers
                scanPredicatePossible = false;
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else if (p.isQualifier()) { // Qualifier in Base Table (FILTER_BASE)
            performQualifierSelectivity(p, QualifierPhase.FILTER_BASE, isIndexOnExpression, defaultSelectivityFactor, SCAN, optimizer);
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else if (PredicateList.isQualifier(p,baseTable,cd,false)) { // Qualifier on Base Table After Index Lookup (FILTER_PROJECTION)
            performQualifierSelectivity(p, QualifierPhase.FILTER_PROJECTION, isIndexOnExpression, defaultSelectivityFactor, TOP, optimizer);
            accumulateExprEvalCost(p);
            collectNoStatsColumnsFromUnaryAndBinaryPred(p);
        }
        else { // Project Restrict Selectivity Filter
            addSelectivity(new DefaultPredicateSelectivity(p, baseTable, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor, optimizer), TOP);
            accumulateExprEvalCost(p);
        }
    }

    /**
     * {@inheritDoc}
     */
    public void generateCost(long numFirstIndexColumnProbes) throws StandardException {

        double baseTableSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder, QualifierPhase.BASE);
        double filterBaseTableSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder,QualifierPhase.BASE,QualifierPhase.FILTER_BASE);
        double projectionSelectivity = computePhaseSelectivity(scanSelectivityHolder, topSelectivityHolder,QualifierPhase.FILTER_PROJECTION);
        double totalSelectivity = computeTotalSelectivity(scanSelectivityHolder, topSelectivityHolder);
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("Generate cost for %s", cd));
            LOG.trace(String.format("Base Table Selectivity %s", baseTableSelectivity));
            LOG.trace(String.format("Filter Base Table Selectivity %s", filterBaseTableSelectivity));
            LOG.trace(String.format("projection Selectivity %s", projectionSelectivity));
            LOG.trace(String.format("total Selectivity %s", totalSelectivity));
        }

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

        double openLatency = scc.getOpenLatency();
        double closeLatency = scc.getCloseLatency();
        double localLatency = scc.getLocalLatency();
        double remoteLatency = scc.getRemoteLatency();
        // IndexPrefixIteration is not currently parallel, so penalize by a factor of the parallelism.
        double remoteCost = (openLatency + closeLatency) +
                (numFirstIndexColumnProbes*2*scc.getParallelism())*remoteLatency*(1+colSizeFactor/1024d) +
                totalRowCount*totalSelectivity*remoteLatency*(1+colSizeFactor/1024d); // Per Kb

        assert openLatency >= 0 : "openLatency cannot be negative -> " + openLatency;
        assert closeLatency >= 0 : "closeLatency cannot be negative -> " + closeLatency;
        assert localLatency >= 0 : "localLatency cannot be negative -> " + localLatency;
        assert remoteLatency >= 0 : "remoteLatency cannot be negative -> " + remoteLatency;
        assert remoteCost >= 0 : "remoteCost cannot be negative -> " + remoteCost;
        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        scanCost.setEstimatedHeapSize((long)(totalRowCount*totalSelectivity*colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long)remoteCost);
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = openLatency+closeLatency;
        assert numFirstIndexColumnProbes >= 0;
        // IndexPrefixIteration is not currently parallel, so penalize by a factor of the parallelism.
        baseCost += (numFirstIndexColumnProbes*2*scc.getParallelism())*localLatency*(1+congAverageWidth/100d);
        baseCost += (totalRowCount*baseTableSelectivity*localLatency*(1+congAverageWidth/100d));
        if (isIndexOnExpression && baseColumnsInLookup == null) {
            // covering index on expression
            // This is a trick to prefer a covering index on expressions over table scan. We have to
            // do it this way because in current optimizer framework, best plan is decided in costing
            // from tables. But an index expression may be evaluated later, for example, as a grouping
            // expression or a select expression.
            baseCost *= 0.9999;
        }
        assert congAverageWidth >= 0 : "congAverageWidth cannot be negative -> " + congAverageWidth;
        assert baseCost >= 0 : "baseCost cannot be negative -> " + baseCost;
        scanCost.setFromBaseTableRows(Math.round(filterBaseTableSelectivity * totalRowCount));
        scanCost.setFromBaseTableCost(baseCost);
        // set how many base table rows to scan
        scanCost.setScannedBaseTableRows(Math.round(baseTableSelectivity * totalRowCount));
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
            lookupCost = totalRowCount*filterBaseTableSelectivity*(openLatency+closeLatency);
            scanCost.setIndexLookupRows(Math.round(filterBaseTableSelectivity*totalRowCount));
            scanCost.setIndexLookupCost(lookupCost+baseCost);
        }
        assert lookupCost >= 0 : "lookupCost cannot be negative -> " + lookupCost;

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
        scanCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        scanCost.setParallelism(scc.getParallelism() != 0 ? scc.getParallelism() : 1);
        scanCost.setLocalCostPerParallelTask((baseCost + lookupCost + projectionCost), scanCost.getParallelism());
        scanCost.setRemoteCostPerParallelTask(scanCost.remoteCost(), scanCost.getParallelism());

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
                                    scanCost.getLocalCost(), scc.getNumPartitions(), scanCost.getLocalCost() / scc.getNumPartitions()));
        }
    }

    /**
     * {@inheritDoc}
     */
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

        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        scanCost.setEstimatedHeapSize((long)(totalRowCount*colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long)(scc.getOpenLatency()+scc.getCloseLatency()+totalRowCount*scc.getRemoteLatency()*(1+colSizeFactor/100d)));
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = scc.getOpenLatency()+scc.getCloseLatency()+(totalRowCount*scc.getLocalLatency()*(1+scc.getConglomerateAvgRowWidth()/100d));
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
            lookupCost = totalRowCount*(scc.getOpenLatency()+scc.getCloseLatency());
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
                                    scanCost.getLocalCost(), scc.getNumPartitions(), scanCost.getLocalCost() / scc.getNumPartitions()));
        }
    }
}
