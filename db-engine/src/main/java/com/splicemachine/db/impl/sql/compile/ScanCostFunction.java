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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.dictionary.ColumnDescriptor;
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
 *
 *
 * @author Scott Fines
 *         Date: 5/15/15
 */
public class ScanCostFunction{
    public static Logger LOG = Logger.getLogger(ScanCostFunction.class);
    private final Optimizable baseTable;
    private final CostEstimate scanCost;
    private final StoreCostController scc;
    private final BitSet scanColumns; //the columns that we are scanning
    private final BitSet lookupColumns; //the columns we are performing a lookup for
    private final BitSet totalColumns;
    private List<SelectivityHolder>[] selectivityHolder; //selectivity elements
    private final int[] keyColumns;
    private final int baseColumnCount;
    private final boolean forUpdate; // Will be used shortly
    private boolean basePredicatePossible = true;

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
     *
     * @param scanColumns
     * @param lookupColumns
     * @param baseTable
     * @param scc
     * @param scanCost
     * @param rowTemplate
     * @param keyColumns
     * @param forUpdate
     * @param resultColumns
     */
    public ScanCostFunction(BitSet scanColumns,
                            BitSet lookupColumns,
                            Optimizable baseTable,
                            StoreCostController scc,
                            CostEstimate scanCost,
                            DataValueDescriptor[] rowTemplate,
                            int[] keyColumns,
                            boolean forUpdate,
                            ResultColumnList resultColumns){
        this.scanColumns = scanColumns;
        this.lookupColumns = lookupColumns;
        this.baseTable=baseTable;
        this.scanCost = scanCost;
        this.scc = scc;
        this.keyColumns = keyColumns;
        this.forUpdate=forUpdate;
        this.selectivityHolder = new List[resultColumns.size()+1];
        this.baseColumnCount = resultColumns.size();
        totalColumns = new BitSet(baseColumnCount);
        totalColumns.or(scanColumns);
        if (lookupColumns != null)
            totalColumns.or(lookupColumns);
    }

    /**
     *
     * Add Selectivity to the selectivity holder.
     *
     * @param holder
     */
    private void addSelectivity(SelectivityHolder holder) {
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
    private List<SelectivityHolder> getSelectivityListForColumn(int colNum) {
        List<SelectivityHolder> holders = selectivityHolder[colNum];
        if (holders == null) {
            holders = new LinkedList<>();
            selectivityHolder[colNum] = holders;
        }
        return holders;
    }

    /**
     *
     * Add Predicate and keep track of the selectivity
     *
     * @param p
     * @throws StandardException
     */
    public void addPredicate(Predicate p, double defaultSelectivityFactor) throws StandardException{
        if (p.isMultiProbeQualifier(keyColumns)) {// MultiProbeQualifier against keys (BASE)
            addSelectivity(new InListSelectivity(scc, p, QualifierPhase.BASE, defaultSelectivityFactor));
        } else if (p.isInQualifier(scanColumns)) // In Qualifier in Base Table (FILTER_PROJECTION) // This is not as expected, needs more research.
            addSelectivity(new InListSelectivity(scc,p, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor));
        else if (p.isInQualifier(lookupColumns)) // In Qualifier against looked up columns (FILTER_PROJECTION)
            addSelectivity(new InListSelectivity(scc,p, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor));
        else if ( (p.isStartKey() || p.isStopKey()) && basePredicatePossible) { // Range Qualifier on Start/Stop Keys (BASE)
            performQualifierSelectivity(p, QualifierPhase.BASE, defaultSelectivityFactor);
            if (!p.isStartKey() || !p.isStopKey()) // Only allows = to further restrict BASE scan numbers
                basePredicatePossible = false;
        }
        else if (p.isQualifier()) // Qualifier in Base Table (FILTER_BASE)
            performQualifierSelectivity(p, QualifierPhase.FILTER_BASE, defaultSelectivityFactor);
        else if (PredicateList.isQualifier(p,baseTable,false)) // Qualifier on Base Table After Index Lookup (FILTER_PROJECTION)
            performQualifierSelectivity(p, QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor);
        else // Project Restrict Selectivity Filter
            addSelectivity(new PredicateSelectivity(p,baseTable,QualifierPhase.FILTER_PROJECTION, defaultSelectivityFactor));
    }

    /**
     *
     * Performs qualifier selectivity based on a phase.
     *
     * @param p
     * @param phase
     * @throws StandardException
     */
    private void performQualifierSelectivity (Predicate p, QualifierPhase phase, double selectivityFactor) throws StandardException {
        if(p.compareWithKnownConstant(baseTable, true) && p.getRelop().getColumnOperand(baseTable) != null) // Range Qualifier
                addRangeQualifier(p,phase, selectivityFactor);
        else // Predicate Cannot Be Transformed to Range, use Predicate Selectivity Defaults
            addSelectivity(new PredicateSelectivity(p,baseTable,phase, selectivityFactor));
    }


    public void generateOneRowCost() throws StandardException {
        // Total Row Count from the Base Conglomerate
        double totalRowCount = 1.0d;
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        scanCost.setEstimatedRowCount(Math.round(totalRowCount));

        double baseTableAverageRowWidth = scc.getBaseTableAvgRowWidth();
        double baseTableColumnSizeFactor = scc.baseTableColumnSizeFactor(totalColumns);
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
        if (lookupColumns == null) {
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
        double projectionCost = totalRowCount * scc.getLocalLatency() * colSizeFactor*1d/1000d;
        scanCost.setProjectionRows(scanCost.getEstimatedRowCount());
        scanCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        scanCost.setLocalCost(baseCost+lookupCost+projectionCost);
        scanCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        scanCost.setLocalCostPerPartition(scanCost.localCost(), scanCost.partitionCount());
        scanCost.setRemoteCostPerPartition(scanCost.remoteCost(), scanCost.partitionCount());
        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("\n" +
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
                            "========================================================\n",
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


    /**
     *
     * Compute the Base Scan Cost by utilizing the passed in StoreCostController
     *
     * @throws StandardException
     */

    public void generateCost() throws StandardException {

        double baseTableSelectivity = computePhaseSelectivity(selectivityHolder,QualifierPhase.BASE);
        double filterBaseTableSelectivity = computePhaseSelectivity(selectivityHolder,QualifierPhase.BASE,QualifierPhase.FILTER_BASE);
        double projectionSelectivity = computePhaseSelectivity(selectivityHolder,QualifierPhase.FILTER_PROJECTION);
        double totalSelectivity = computeTotalSelectivity(selectivityHolder);

        assert filterBaseTableSelectivity >= 0 && filterBaseTableSelectivity <= 1.0:"filterBaseTableSelectivity Out of Bounds -> " + filterBaseTableSelectivity;
        assert baseTableSelectivity >= 0 && baseTableSelectivity <= 1.0:"baseTableSelectivity Out of Bounds -> " + baseTableSelectivity;
        assert projectionSelectivity >= 0 && projectionSelectivity <= 1.0:"projectionSelectivity Out of Bounds -> " + projectionSelectivity;
        assert totalSelectivity >= 0 && totalSelectivity <= 1.0:"totalSelectivity Out of Bounds -> " + totalSelectivity;

        // Total Row Count from the Base Conglomerate
        double totalRowCount = scc.baseRowCount();
        assert totalRowCount >= 0 : "totalRowCount cannot be negative -> " + totalRowCount;
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        scanCost.setEstimatedRowCount(Math.round(totalRowCount*totalSelectivity));

        double baseTableAverageRowWidth = scc.getBaseTableAvgRowWidth();
        double baseTableColumnSizeFactor = scc.baseTableColumnSizeFactor(totalColumns);

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
        scanCost.setEstimatedHeapSize((long)(totalRowCount*totalSelectivity*colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long)remoteCost);
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = scc.getConglomerateAvgRowWidth();
        double baseCost = openLatency+closeLatency+(totalRowCount*baseTableSelectivity*localLatency*(1+congAverageWidth/100d));
        assert congAverageWidth >= 0 : "congAverageWidth cannot be negative -> " + congAverageWidth;
        assert baseCost >= 0 : "baseCost cannot be negative -> " + baseCost;
        scanCost.setFromBaseTableRows(Math.round(filterBaseTableSelectivity * totalRowCount));
        scanCost.setFromBaseTableCost(baseCost);
        // set how many base table rows to scan
        scanCost.setScannedBaseTableRows(Math.round(baseTableSelectivity * totalRowCount));
        double lookupCost;
        if (lookupColumns == null) {
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
            projectionCost = totalRowCount * filterBaseTableSelectivity * localLatency * colSizeFactor*1d/1000d;
            scanCost.setProjectionRows(Math.round(scanCost.getEstimatedRowCount()));
            scanCost.setProjectionCost(lookupCost+baseCost+projectionCost);
        }
        assert projectionCost >= 0 : "projectionCost cannot be negative -> " + projectionCost;

        double localCost = baseCost+lookupCost+projectionCost;
        assert localCost >= 0 : "localCost cannot be negative -> " + localCost;
        scanCost.setLocalCost(localCost);
        scanCost.setNumPartitions(scc.getNumPartitions() != 0 ? scc.getNumPartitions() : 1);
        scanCost.setLocalCostPerPartition((baseCost + lookupCost + projectionCost), scanCost.partitionCount());
        scanCost.setRemoteCostPerPartition(scanCost.remoteCost(), scanCost.partitionCount());

        if (LOG.isTraceEnabled()) {
            LOG.trace(String.format("\n" +
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
            "========================================================\n",
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

    /**
     *
     * Computing the total selectivity.  All conglomerates need to have the same total selectivity.
     *
     * @param selectivityHolder
     * @return
     * @throws StandardException
     */
    public static double computeTotalSelectivity(List<SelectivityHolder>[] selectivityHolder) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        for (List<SelectivityHolder> aSelectivityHolder : selectivityHolder) {
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
     * @param selectivityHolder
     * @param phases
     * @return
     * @throws StandardException
     */
    public static double computePhaseSelectivity(List<SelectivityHolder>[] selectivityHolder,QualifierPhase... phases) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        for (List<SelectivityHolder> aSelectivityHolder : selectivityHolder) {
            if (aSelectivityHolder != null) {
                for (SelectivityHolder holder : aSelectivityHolder) {
                    for (QualifierPhase phase : phases) {
                        if (holder.getPhase().equals(phase))
                            holders.add(holder); // Only add Phased Qualifiers
                    }
                }
            }
        }
        Collections.sort(holders);
        return computeSelectivity(totalSelectivity,holders);
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

    private boolean addRangeQualifier(Predicate p,QualifierPhase phase, double selectivityFactor) throws StandardException{
        DataValueDescriptor value=p.getCompareValue(baseTable);
        RelationalOperator relop=p.getRelop();
        ColumnReference cr = relop.getColumnOperand(baseTable);
        ColumnDescriptor columnDescriptor = cr.getSource().getTableColumnDescriptor();
        boolean useExtrapolation = false;
        if (columnDescriptor != null)
            useExtrapolation = columnDescriptor.getUseExtrapolation() != 0;

        int colNum = cr.getColumnNumber();
        int relationalOperator = relop.getOperator();
        List<SelectivityHolder> columnHolder = getSelectivityListForColumn(colNum);
        OP_SWITCH: switch(relationalOperator){
            case RelationalOperator.EQUALS_RELOP:
                columnHolder.add(new RangeSelectivity(scc,value,value,true,true,colNum,phase, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.NOT_EQUALS_RELOP:
                columnHolder.add(new NotEqualsSelectivity(scc,colNum,phase,value, selectivityFactor, useExtrapolation));
                break;
            case RelationalOperator.IS_NULL_RELOP:
                columnHolder.add(new NullSelectivity(scc,colNum,phase));
                break;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                columnHolder.add(new NotNullSelectivity(scc,colNum,phase));
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
                columnHolder.add(new RangeSelectivity(scc,value,null,true,true,colNum,phase, selectivityFactor, useExtrapolation));
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
                columnHolder.add(new RangeSelectivity(scc,value,null,false,true,colNum,phase, selectivityFactor, useExtrapolation));
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
                columnHolder.add(new RangeSelectivity(scc,null,value,true,true,colNum,phase, selectivityFactor, useExtrapolation));
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
                columnHolder.add(new RangeSelectivity(scc,null,value,true,false,colNum,phase, selectivityFactor, useExtrapolation));
                break;
            default:
                throw new RuntimeException("Unknown Qualifier Type");
         }
        return true;
    }


}
