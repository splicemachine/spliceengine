/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.catalog.IndexDescriptor;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.Parser;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.util.*;

/**
 * A Mutable object representing a builder for the Table-scan cost.
 *
 * In practice, the act of estimating the cost of a table scan is really quite convoluted, as it depends
 * a great deal on the type and nature of the predicates which are passed in. To resolve this complexity, and
 * to support flexible behaviors, we use this builder pattern instead of direct coding.
 *
 * An access path of a base table includes at maximum three parts:
 * 1) Scanning the base conglomerate. This can be a scan on a base table or an index.
 * 2) Looking up a base row according to an index. This happens only when the index is not covering.
 * 3) Projecting columns away and further reject rows by evaluating remaining predicates.
 *
 * With the support of index on expressions, columns scanned in phase 1 are not necessarily base table columns.
 * Further, the numbers of scanned columns are not bound to the number of base table columns, either. For this
 * reason, we split the list of SelectivityHolder into two parts. SCAN is for phase 1 and TOP is for phase 2
 * and 3.
 *
 * @author Scott Fines
 *         Date: 5/15/15
 */
public class ScanCostFunction{
    public static final Logger LOG = Logger.getLogger(ScanCostFunction.class);

    private static final int SCAN = 0;  // qualifier phase: BASE, FILTER_BASE
    private static final int TOP  = 1;  // qualifier phase: FILTER_PROJECTION

    private final Optimizable baseTable;
    private final IndexDescriptor indexDescriptor;
    private final boolean isIndex;
    private final boolean isPrimaryKey;
    private final boolean isIndexOnExpression;

    private final CostEstimate totalCost;
    private final StoreCostController scc;

    // resultColumns from the base table
    // at this point, this seems to be always full column list because no access path is chosen
    private final ResultColumnList resultColumns;

    // base columns returned from scanning phase
    private final BitSet baseColumnsInScan;

    // base columns returned from looking up phase
    private final BitSet baseColumnsInLookup;

    // whether it's possible to consider further scan predicates or not
    private boolean scanPredicatePossible = true;

    // positions of columns used in estimating selectivity but missing real statistics
    private final HashSet<Integer> usedNoStatsColumnIds;

    // will be used shortly
    private final boolean forUpdate;

    // selectivity elements for scanning phase
    private final List<SelectivityHolder>[] scanSelectivityHolder;

    // selectivity elements for look up and projection phase
    private final List<SelectivityHolder>[] topSelectivityHolder;

    // for base tables, this is null
    // for normal indexes and primary keys, stores column references to the base table
    // for indexes on expressions, stores the ASTs of index expressions in their defined order
    private final ValueNode[] indexColumns;

    // cost of evaluating all expressions used in added predicates per row
    private double exprEvalCostPerRow = 0.0f;

    /**
     *
     *
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
     *
     * @param baseTable
     * @param cd
     * @param scc
     * @param totalCost
     * @param resultColumns
     * @param scanRowTemplate
     * @param forUpdate
     * @param usedNoStatsColumnIds
     */
    public ScanCostFunction(Optimizable baseTable,
                            ConglomerateDescriptor cd,
                            StoreCostController scc,
                            CostEstimate totalCost,
                            ResultColumnList resultColumns,
                            DataValueDescriptor[] scanRowTemplate,
                            BitSet baseColumnsInScan,
                            BitSet baseColumnsInLookup,
                            boolean forUpdate,
                            HashSet<Integer> usedNoStatsColumnIds) throws StandardException {
        this.baseTable=baseTable;
        this.indexDescriptor = cd.getIndexDescriptor();
        this.isIndex = cd.isIndex();
        this.isPrimaryKey = cd.isPrimaryKey();
        this.isIndexOnExpression = isIndex && indexDescriptor.isOnExpression();
        this.totalCost = totalCost;
        this.scc = scc;
        this.resultColumns = resultColumns;
        this.baseColumnsInScan = baseColumnsInScan;
        this.baseColumnsInLookup = baseColumnsInLookup;
        this.forUpdate=forUpdate;
        this.usedNoStatsColumnIds = usedNoStatsColumnIds;

        /* We always allocate one extra column in a selectivity holder array because column
         * positions are 1-based, and slot index 0 is used for storing predicates fall into
         * default selectivity estimation.
         */
        int numColumnsInScan;
        int numColumnsInTop;
        if (isIndexOnExpression) {
            /* For an index row, scanRowTemplate has one extra column for the source row key.
             * It's impossible to have predicates on source row key, so we can safely use
             * slot index 0 for storing default selectivity estimation.
             */
            numColumnsInScan = scanRowTemplate.length;
            numColumnsInTop = baseColumnsInLookup == null ? numColumnsInScan /* covering index */ : resultColumns.size() + 1;
        } else {
            numColumnsInScan = resultColumns.size() + 1;
            numColumnsInTop = numColumnsInScan;
        }
        this.scanSelectivityHolder = new List[numColumnsInScan];
        this.topSelectivityHolder = new List[numColumnsInTop];

        this.indexColumns = getIndexColumns();
        correctBaseColumnInfo();
    }

    private ValueNode[] getIndexColumns() throws StandardException {
        if (!isIndex && !isPrimaryKey) {
            return null;
        }

        ValueNode[] indexColumns;
        if (isIndexOnExpression) {
            assert baseTable instanceof QueryTreeNode;
            LanguageConnectionContext lcc = ((QueryTreeNode)baseTable).getLanguageConnectionContext();
            CompilerContext newCC = lcc.pushCompilerContext();
            Parser p = newCC.getParser();

            String[] exprTexts = indexDescriptor.getExprTexts();
            indexColumns = new ValueNode[exprTexts.length];
            for (int i = 0; i < exprTexts.length; i++) {
                ValueNode exprAst = (ValueNode) p.parseSearchCondition(exprTexts[i]);
                PredicateList.setTableNumber(exprAst, baseTable);
                indexColumns[i] = exprAst;
            }
            lcc.popCompilerContext(newCC);
        } else {
            int[] keyColumns = indexDescriptor.baseColumnPositions();
            indexColumns = new ValueNode[keyColumns.length];
            for (int i = 0; i < keyColumns.length; i++) {
                ColumnReference cr = resultColumns.getResultColumn(keyColumns[i]).getColumnReference(null);
                cr.setTableNumber(baseTable.getTableNumber());
                indexColumns[i] = cr;
            }
        }
        return indexColumns;
    }

    private void correctBaseColumnInfo() {
        if (isIndexOnExpression) {
            if (baseColumnsInLookup != null) {
                // for a non-covering index defined on expressions, we potentially look up all base columns
                baseColumnsInLookup.or(baseColumnsInScan);
            }
            // no base column is scanned when scanning an index defined on expressions
            baseColumnsInScan.clear();
        }
    }

    private int getTotalNumberOfBaseColumnsInvolved() {
        BitSet result = new BitSet(resultColumns.size());
        result.or(baseColumnsInScan);
        if (baseColumnsInLookup != null) {
            result.or(baseColumnsInLookup);
        }
        return result.cardinality();
    }

    private int mapQualifierPhaseToPhase(QualifierPhase qPhase) {
        switch (qPhase) {
            case BASE:
            case FILTER_BASE:
                return SCAN;
            case FILTER_PROJECTION:
                return TOP;
            default:
                throw new RuntimeException("invalid QualifierPhase value");
        }
    }

    /**
     *
     * Add Selectivity to the selectivity holder.
     *
     * @param holder
     */
    private void addSelectivity(SelectivityHolder holder, int phase) {
        List<SelectivityHolder>[] selectivityHolder =
                phase == SCAN ? scanSelectivityHolder : topSelectivityHolder;
        List<SelectivityHolder> holders = selectivityHolder[holder.getColNum()];
        if (holders == null) {
            holders = new LinkedList<>();
            selectivityHolder[holder.getColNum()] = holders;
        }
        holders.add(holder);
    }

    /**
     *
     * Retrieve the selectivity for the columns.
     *
     * @param colNum
     * @return
     */
    private List<SelectivityHolder> getSelectivityListForColumn(int colNum, int phase) {
        List<SelectivityHolder>[] selectivityHolder =
                phase == SCAN ? scanSelectivityHolder : topSelectivityHolder;
        List<SelectivityHolder> holders = selectivityHolder[colNum];
        if (holders == null) {
            holders = new LinkedList<>();
            selectivityHolder[colNum] = holders;
        }
        return holders;
    }

    private void collectNoStatsColumnsFromInListPred(Predicate p) throws StandardException {
        if (p.getSourceInList() != null) {
            for (Object o : p.getSourceInList().leftOperandList) {
                List<ColumnReference> crList = ((ValueNode)o).getHashableJoinColumnReference();
                for (ColumnReference cr : crList) {
                    if (!scc.useRealBaseColumnStatistics(cr.getColumnNumber()))
                        usedNoStatsColumnIds.add(cr.getColumnNumber());
                }
            }
        }
    }

    private void collectNoStatsColumnsFromUnaryAndBinaryPred(Predicate p) {
        if (p.getRelop() != null) {
            ColumnReference cr = p.getRelop().getColumnOperand(baseTable);
            if (cr != null && !scc.useRealBaseColumnStatistics(cr.getColumnNumber())) {
                usedNoStatsColumnIds.add(cr.getColumnNumber());
            }
        }
    }

    private void accumulateExprEvalCost(Predicate p) throws StandardException {
        exprEvalCostPerRow += p.getAndNode().getLeftOperand().getBaseOperationCost();
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
        else if (PredicateList.isQualifier(p,baseTable,indexDescriptor,false)) { // Qualifier on Base Table After Index Lookup (FILTER_PROJECTION)
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
     * Performs qualifier selectivity based on a qualifierPhase.
     *
     * @param p
     * @param qualifierPhase
     * @throws StandardException
     */
    private void performQualifierSelectivity (Predicate p, QualifierPhase qualifierPhase, boolean forIndexExpr, double selectivityFactor, int phase) throws StandardException {
        if(p.compareWithKnownConstant(baseTable, true) &&
                (p.getRelop().getColumnOperand(baseTable) != null ||
                        (forIndexExpr && p.getRelop().getExpressionOperand(baseTable.getTableNumber(), -1, (FromTable)baseTable, true) != null) && p.getIndexPosition() >= 0))
        {
            // Range Qualifier
            addRangeQualifier(p, qualifierPhase, forIndexExpr, selectivityFactor);
        }
        else if(p.isBetween() && forIndexExpr && ((BetweenOperatorNode) p.getAndNode().getLeftOperand()).compareWithKnownConstant(true)) {
            addRangeQualifier(p, qualifierPhase, true, selectivityFactor);
        }
        else {
            // Predicate Cannot Be Transformed to Range, use Predicate Selectivity Defaults
            addSelectivity(new DefaultPredicateSelectivity(p, baseTable, qualifierPhase, selectivityFactor), phase);
        }
    }


    public void generateOneRowCost() throws StandardException {
        // Total Row Count from the Base Conglomerate
        double totalRowCount = 1.0d;
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        totalCost.setEstimatedRowCount(Math.round(totalRowCount));

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
        totalCost.setEstimatedHeapSize((long)(totalRowCount*colSizeFactor));
        // Should be the same for each conglomerate
        totalCost.setRemoteCost((long)(scc.getOpenLatency()+scc.getCloseLatency()+totalRowCount*scc.getRemoteLatency()*(1+colSizeFactor/100d)));
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = scc.getOpenLatency()+scc.getCloseLatency()+(totalRowCount*scc.getLocalLatency()*(1+scc.getConglomerateAvgRowWidth()/100d));
        totalCost.setFromBaseTableRows(totalRowCount);
        totalCost.setFromBaseTableCost(baseCost);
        totalCost.setScannedBaseTableRows(totalRowCount);
        double lookupCost;
        if (baseColumnsInLookup == null) {
            lookupCost = 0.0d;

            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            totalCost.setIndexLookupRows(-1.0d);
            totalCost.setIndexLookupCost(-1.0d);
        } else {
            lookupCost = totalRowCount*(scc.getOpenLatency()+scc.getCloseLatency());
            totalCost.setIndexLookupRows(totalRowCount);
            totalCost.setIndexLookupCost(lookupCost+baseCost);
        }
        double projectionCost = totalRowCount * (scc.getLocalLatency() * colSizeFactor*1d/1000d + exprEvalCostPerRow);
        totalCost.setProjectionRows(totalCost.getEstimatedRowCount());
        totalCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        totalCost.setLocalCost(baseCost+lookupCost+projectionCost);
        totalCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        totalCost.setLocalCostPerPartition(totalCost.localCost(), totalCost.partitionCount());
        totalCost.setRemoteCostPerPartition(totalCost.remoteCost(), totalCost.partitionCount());
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
                    totalCost.rowCount(), totalCost.getEstimatedHeapSize(), congAverageWidth,
                    totalCost.getFromBaseTableRows(), totalCost.getScannedBaseTableRows(),
                    totalCost.getFromBaseTableCost(), totalCost.getRemoteCost(),
                    totalCost.getIndexLookupRows(), totalCost.getIndexLookupCost(),
                    totalCost.getProjectionRows(), totalCost.getProjectionCost(),
                    totalCost.getLocalCost(), scc.getNumPartitions(), totalCost.getLocalCost()/scc.getNumPartitions()));
        }
    }


    /**
     *
     * Compute the Base Scan Cost by utilizing the passed in StoreCostController
     *
     * @throws StandardException
     */

    public void generateCost() throws StandardException {

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
        totalCost.setEstimatedRowCount(Math.round(totalRowCount*totalSelectivity));

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
        double remoteCost = openLatency + closeLatency + totalRowCount*totalSelectivity*remoteLatency*(1+colSizeFactor/1024d); // Per Kb

        assert openLatency >= 0 : "openLatency cannot be negative -> " + openLatency;
        assert closeLatency >= 0 : "closeLatency cannot be negative -> " + closeLatency;
        assert localLatency >= 0 : "localLatency cannot be negative -> " + localLatency;
        assert remoteLatency >= 0 : "remoteLatency cannot be negative -> " + remoteLatency;
        assert remoteCost >= 0 : "remoteCost cannot be negative -> " + remoteCost;
        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        totalCost.setEstimatedHeapSize((long)(totalRowCount*totalSelectivity*colSizeFactor));
        // Should be the same for each conglomerate
        totalCost.setRemoteCost((long)remoteCost);
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = openLatency+closeLatency+(totalRowCount*baseTableSelectivity*localLatency*(1+congAverageWidth/100d));
        assert congAverageWidth >= 0 : "congAverageWidth cannot be negative -> " + congAverageWidth;
        assert baseCost >= 0 : "baseCost cannot be negative -> " + baseCost;
        totalCost.setFromBaseTableRows(Math.round(filterBaseTableSelectivity * totalRowCount));
        totalCost.setFromBaseTableCost(baseCost);
        // set how many base table rows to scan
        totalCost.setScannedBaseTableRows(Math.round(baseTableSelectivity * totalRowCount));
        double lookupCost;
        if (baseColumnsInLookup == null) {
            lookupCost = 0.0d;
            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            totalCost.setIndexLookupRows(-1.0d);
            totalCost.setIndexLookupCost(-1.0d);
        } else {
            lookupCost = totalRowCount*filterBaseTableSelectivity*(openLatency+closeLatency);
            totalCost.setIndexLookupRows(Math.round(filterBaseTableSelectivity*totalRowCount));
            totalCost.setIndexLookupCost(lookupCost+baseCost);
        }
        assert lookupCost >= 0 : "lookupCost cannot be negative -> " + lookupCost;

        double projectionCost;
        if (projectionSelectivity == 1.0d) {
            projectionCost = 0.0d;
            /* we need to reset the lookup cost here, otherwise, we may see the lookup cost
               from the previous access path
               see how the cost and rowcount are initialized in SimpleCostEstimate
             */
            totalCost.setProjectionRows(-1.0d);
            totalCost.setProjectionCost(-1.0d);
        } else {
            projectionCost = totalRowCount * filterBaseTableSelectivity * (localLatency * colSizeFactor*1d/1000d + exprEvalCostPerRow);
            totalCost.setProjectionRows((double) totalCost.getEstimatedRowCount());
            totalCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        }
        assert projectionCost >= 0 : "projectionCost cannot be negative -> " + projectionCost;

        double localCost = baseCost+lookupCost+projectionCost;
        assert localCost >= 0 : "localCost cannot be negative -> " + localCost;
        totalCost.setLocalCost(localCost);
        totalCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        totalCost.setLocalCostPerPartition((baseCost + lookupCost + projectionCost), totalCost.partitionCount());
        totalCost.setRemoteCostPerPartition(totalCost.remoteCost(), totalCost.partitionCount());

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
            totalCost.rowCount(), totalCost.getEstimatedHeapSize(), congAverageWidth,
            totalCost.getFromBaseTableRows(), totalCost.getScannedBaseTableRows(),
            totalCost.getFromBaseTableCost(), totalCost.getRemoteCost(),
            totalCost.getIndexLookupRows(), totalCost.getIndexLookupCost(),
            totalCost.getProjectionRows(), totalCost.getProjectionCost(),
            totalCost.getLocalCost(), scc.getNumPartitions(), totalCost.getLocalCost()/scc.getNumPartitions()));
        }
    }

    /**
     *
     * Computing the total selectivity.  All conglomerates need to have the same total selectivity.
     *
     * @param scanSelectivityHolder
     * @param topSelectivityHolder
     * @return
     * @throws StandardException
     */
    public static double computeTotalSelectivity(List<SelectivityHolder>[] scanSelectivityHolder,
                                                 List<SelectivityHolder>[] topSelectivityHolder) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        for (List<SelectivityHolder> aSelectivityHolder : scanSelectivityHolder) {
            if (aSelectivityHolder != null)
                holders.addAll(aSelectivityHolder);
        }
        for (List<SelectivityHolder> aSelectivityHolder : topSelectivityHolder) {
            if (aSelectivityHolder != null)
                holders.addAll(aSelectivityHolder);
        }
        Collections.sort(holders);
        return computeSelectivity(totalSelectivity,holders);
    }


    /**
     *
     * Gathers the selectivities for the phases and sorts them ascending (most selective first) and then supplied them to computeSelectivity.
     *
     * @param scanSelectivityHolder
     * @param topSelectivityHolder
     * @param phases
     * @return
     * @throws StandardException
     */
    public static double computePhaseSelectivity(List<SelectivityHolder>[] scanSelectivityHolder,
                                                 List<SelectivityHolder>[] topSelectivityHolder,
                                                 QualifierPhase... phases) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        collectSelectivityHolders(scanSelectivityHolder, holders, phases);
        collectSelectivityHolders(topSelectivityHolder, holders, phases);
        Collections.sort(holders);
        return computeSelectivity(totalSelectivity,holders);
    }

    private static void collectSelectivityHolders(List<SelectivityHolder>[] from, List<SelectivityHolder> to,
                                                  QualifierPhase... phases) {
        for (List<SelectivityHolder> aSelectivityHolder : from) {
            if (aSelectivityHolder != null) {
                for (SelectivityHolder holder : aSelectivityHolder) {
                    for (QualifierPhase phase : phases) {
                        if (holder.getPhase().equals(phase))
                            to.add(holder); // Only add Phased Qualifiers
                    }
                }
            }
        }
    }

    /**
     *
     * Helper method to compute increasing sqrt levels.
     *
     * @param selectivity
     * @param holders
     * @return
     * @throws StandardException
     */
    public static double computeSelectivity(double selectivity, List<SelectivityHolder> holders) throws StandardException {
        for (int i = 0; i< holders.size();i++) {
            selectivity = computeSqrtLevel(selectivity,i,holders.get(i));
        }
        return selectivity;
    }

    /**
     *
     * Compute SQRT selectivity based on the level.
     *
     * @param selectivity
     * @param level
     * @param holder
     * @return
     * @throws StandardException
     */
    public static double computeSqrtLevel(double selectivity, int level, SelectivityHolder holder) throws StandardException {
        if (level ==0) {
            selectivity *= holder.getSelectivity();
            return selectivity;
        }
        double incrementalSelectivity = 0.0d;
        incrementalSelectivity += holder.getSelectivity();
        for (int i =1;i<=level;i++)
            incrementalSelectivity=Math.sqrt(incrementalSelectivity);
        selectivity*=incrementalSelectivity;
        return selectivity;
    }

    /**
     *
     * Method to combine range qualifiers a>12 and a< 15 -> range qualifier (12<a<15)
     *
     * @param p
     * @param phase
     * @return
     * @throws StandardException
     */

    private boolean addRangeQualifier(Predicate p, QualifierPhase phase, boolean forIndexExpr, double selectivityFactor)
            throws StandardException
    {
        RelationalOperator relop=p.getRelop();
        boolean useExtrapolation = false;

        int colNum;
        if (forIndexExpr && p.getIndexPosition() >= 0) {
            colNum = p.getIndexPosition() + 1;
        } else {
            ColumnReference cr = relop.getColumnOperand(baseTable);
            ColumnDescriptor columnDescriptor = cr.getSource().getTableColumnDescriptor();
            if (columnDescriptor != null)
                useExtrapolation = columnDescriptor.getUseExtrapolation() != 0;

            colNum = cr.getColumnNumber();
        }

        List<SelectivityHolder> columnHolder = getSelectivityListForColumn(colNum, mapQualifierPhaseToPhase(phase));

        if (p.isBetween()) {
            BetweenOperatorNode bon = (BetweenOperatorNode) p.getAndNode().getLeftOperand();
            DataValueDescriptor start = ((ValueNode) bon.getRightOperandList().elementAt(0)).getKnownConstantValue();
            DataValueDescriptor stop  = ((ValueNode) bon.getRightOperandList().elementAt(1)).getKnownConstantValue();
            columnHolder.add(new RangeSelectivity(scc, start, stop, true, true, forIndexExpr, colNum, phase, selectivityFactor, useExtrapolation));
            return true;
        }

        DataValueDescriptor value=p.getCompareValue(baseTable);
        int relationalOperator = relop.getOperator();
        OP_SWITCH: switch(relationalOperator){
            case RelationalOperator.EQUALS_RELOP:
                columnHolder.add(new RangeSelectivity(scc,value,value,true,true, forIndexExpr, colNum,phase, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.NOT_EQUALS_RELOP:
                columnHolder.add(new NotEqualsSelectivity(scc, forIndexExpr, colNum, phase, value, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.IS_NULL_RELOP:
                columnHolder.add(new NullSelectivity(scc, forIndexExpr, colNum, phase));
                break;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                columnHolder.add(new NotNullSelectivity(scc, forIndexExpr, colNum, phase));
                break;
            case RelationalOperator.GREATER_EQUALS_RELOP:
                for(SelectivityHolder sh: columnHolder){
                    if (!sh.isRangeSelectivity())
                        continue;
                    RangeSelectivity rq = (RangeSelectivity) sh;
                    if(rq.start==null){
                        rq.start = value;
                        rq.includeStart = true;
                        break OP_SWITCH;
                    }
                }
                columnHolder.add(new RangeSelectivity(scc,value,null,true,true, forIndexExpr, colNum, phase, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.GREATER_THAN_RELOP:
                for(SelectivityHolder sh: columnHolder){
                    if (!sh.isRangeSelectivity())
                        continue;
                    RangeSelectivity rq = (RangeSelectivity) sh;
                    if(rq.start==null){
                        rq.start = value;
                        rq.includeStart = false;
                        break OP_SWITCH;
                    }
                }
                columnHolder.add(new RangeSelectivity(scc,value,null,false,true, forIndexExpr, colNum, phase, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.LESS_EQUALS_RELOP:
                for(SelectivityHolder sh: columnHolder){
                    if (!sh.isRangeSelectivity())
                        continue;
                    RangeSelectivity rq = (RangeSelectivity) sh;
                    if(rq.stop==null){
                        rq.stop = value;
                        rq.includeStop = true;
                        break OP_SWITCH;
                    }
                }
                columnHolder.add(new RangeSelectivity(scc,null,value,true,true, forIndexExpr, colNum, phase, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.LESS_THAN_RELOP:
                for(SelectivityHolder sh: columnHolder){
                    if (!sh.isRangeSelectivity())
                        continue;
                    RangeSelectivity rq = (RangeSelectivity) sh;
                    if(rq.stop==null){
                        rq.stop = value;
                        rq.includeStop = false;
                        break OP_SWITCH;
                    }
                }
                columnHolder.add(new RangeSelectivity(scc,null,value,true,false, forIndexExpr, colNum, phase, selectivityFactor, useExtrapolation));
                break;
            default:
                throw new RuntimeException("Unknown Qualifier Type");
         }
        return true;
    }
}
