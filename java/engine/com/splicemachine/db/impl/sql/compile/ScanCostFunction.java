package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import java.util.*;

/**
 * A Mutable object representing a builder for the Table-scan cost.
 *
 * In practice, the act of estimating the cost of a table scan is really quite convoluted, as it depends
 * a great deal on the type and nature of the predicates which are passed in. To resolve this complexity, and
 * to support flexible behaviors, we use this builder pattern instead of direct coding.
 *
 * @author Scott Fines
 *         Date: 5/15/15
 */
public class ScanCostFunction{
    private final Optimizable baseTable;
    private final CostEstimate scanCost;
    private final StoreCostController storeCost;
    private final BitSet scanColumns; //the columns that we are scanning
    private final BitSet lookupColumns; //the columns we are performing a lookup for
    private final BitSet totalColumns;
    private List<SelectivityHolder>[] selectivityHolder; //selectivity elements
    private final int[] keyColumns;
    private final int baseColumnCount;
    private final long baseRowCount;
    private final boolean forUpdate;
    private final DataValueDescriptor[] rowTemplate;
    private final int startOperator;
    private final int stopOperator;
    private transient boolean baseCostComputed;
    private transient boolean lookupCostComputed;
    private transient boolean outputCostComputed;

    /**
     *
     * Applies Predicates at 3 levels (BASE,BASE_FILTER,PROJECTION_FILTER).
     *
     * @param scanColumns
     * @param lookupColumns
     * @param baseTable
     * @param storeCost
     * @param scanCost
     * @param rowTemplate
     * @param keyColumns
     * @param baseRowCount
     * @param forUpdate
     * @param startOperator
     * @param stopOperator
     * @param resultColumns
     */
    public ScanCostFunction(BitSet scanColumns,
                            BitSet lookupColumns,
                            Optimizable baseTable,
                            StoreCostController storeCost,
                            CostEstimate scanCost,
                            DataValueDescriptor[] rowTemplate,
                            int[] keyColumns,
                            long baseRowCount,
                            boolean forUpdate,
                            int startOperator,
                            int stopOperator,
                            ResultColumnList resultColumns){
        this.scanColumns = scanColumns;
        this.lookupColumns = lookupColumns;
        this.baseTable=baseTable;
        this.scanCost = scanCost;
        this.storeCost = storeCost;
        this.keyColumns = keyColumns;
        this.baseRowCount=baseRowCount;
        this.forUpdate=forUpdate;
        this.startOperator=startOperator;
        this.stopOperator=stopOperator;
        this.selectivityHolder = new List[resultColumns.size()+1];
        this.rowTemplate = rowTemplate;
        this.baseColumnCount = resultColumns.size();
        totalColumns = new BitSet(baseColumnCount);
        totalColumns.or(scanColumns);
        if (lookupColumns != null)
            totalColumns.or(lookupColumns);
    }

    private void addSelectivity(SelectivityHolder holder) {
        List<SelectivityHolder> holders = selectivityHolder[holder.getColNum()];
        if (holders == null) {
            holders = new LinkedList<SelectivityHolder>();
            selectivityHolder[holder.getColNum()] = holders;
        }
        holders.add(holder);
    }

    private List<SelectivityHolder> getSelectivityListForColumn(int colNum) {
        List<SelectivityHolder> holders = selectivityHolder[colNum];
        if (holders == null) {
            holders = new LinkedList<SelectivityHolder>();
            selectivityHolder[colNum] = holders;
        }
        return holders;
    }

    public void addPredicate(Predicate p) throws StandardException{
        if (p.isMultiProbeQualifier(keyColumns)) // MultiProbeQualifier against keys (BASE)
            addSelectivity(new InListSelectivity(storeCost,p,QualifierPhase.BASE));
        else if (p.isStartKey() || p.isStopKey()) // Range Qualifier on Start/Stop Keys (BASE)
            performQualifierSelectivity(p,QualifierPhase.BASE);
        else if (p.isQualifier()) // Qualifier in Base Table (FILTER_BASE)
            performQualifierSelectivity(p, QualifierPhase.FILTER_BASE);
        else if (PredicateList.isQualifier(p,baseTable,false)) // Qualifier on Base Table After Index Lookup (FILTER_PROJECTION)
            performQualifierSelectivity(p, QualifierPhase.FILTER_PROJECTION);
        else // Project Restrict Selectivity Filter
            addSelectivity(new PredicateSelectivity(p,baseTable,QualifierPhase.FILTER_PROJECTION));
    }

    private void performQualifierSelectivity (Predicate p, QualifierPhase phase) throws StandardException {
        if (p.isInQualifier()) // In List Qualifier
                addSelectivity(new InListSelectivity(storeCost,p, phase));
        else if(p.compareWithKnownConstant(baseTable, true) && p.getRelop().getColumnOperand(baseTable) != null) // Range Qualifier
                addRangeQualifier(p,phase);
        else // Predicate Cannot Be Transformed to Range, use Predicate Selectivity Defaults
            addSelectivity(new PredicateSelectivity(p,baseTable,phase));
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

        double totalRowCount = storeCost.rowCount();
        // Rows Returned is always the totalSelectivity (Conglomerate Independent)
        scanCost.setEstimatedRowCount((long) (totalRowCount*totalSelectivity));



        // We use the base table so the estimated heap size and remote cost are the same for all conglomerates
        double colSizeFactor = storeCost.getBaseTableAvgRowWidth()*storeCost.baseTableColumnSizeFactor(totalColumns);
        // Heap Size is the avg row width of the columns for the base table*total rows
        // Average Row Width
        // This should be the same for every conglomerate path
        scanCost.setEstimatedHeapSize((long)(totalRowCount*totalSelectivity*colSizeFactor));
        // Should be the same for each conglomerate
        scanCost.setRemoteCost((long)(totalRowCount*totalSelectivity*storeCost.getRemoteLatency()*colSizeFactor));
        // Base Cost + LookupCost + Projection Cost
        double congAverageWidth = storeCost.getConglomerateAvgRowWidth();
        double congColSizeFactor = congAverageWidth*storeCost.conglomerateColumnSizeFactor(scanColumns);
        double baseCost = totalRowCount*baseTableSelectivity*storeCost.getLocalLatency()*storeCost.getConglomerateAvgRowWidth();

        double lookupCost = lookupColumns == null?0.0d:
                totalRowCount*filterBaseTableSelectivity*storeCost.getRemoteLatency()*storeCost.getBaseTableAvgRowWidth()*storeCost.baseTableColumnSizeFactor(lookupColumns);
        double projectionCost = projectionSelectivity == 1.0d?0.0d:totalRowCount*filterBaseTableSelectivity*storeCost.getLocalLatency()*colSizeFactor;
        scanCost.setLocalCost(baseCost+lookupCost+projectionCost);
        scanCost.setNumPartitions(storeCost.getNumPartitions());
    }

    public static double computeTotalSelectivity(List<SelectivityHolder>[] selectivityHolder) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        for (int i = 0; i< selectivityHolder.length; i++) {
            if (selectivityHolder[i] != null)
                holders.addAll(selectivityHolder[i]);
        }
        Collections.sort(holders);
        return computeSelectivity(totalSelectivity,holders);
    }

    public static double computePhaseSelectivity(List<SelectivityHolder>[] selectivityHolder,QualifierPhase... phases) throws StandardException {
        double totalSelectivity = 1.0d;
        List<SelectivityHolder> holders = new ArrayList();
        for (int i = 0; i< selectivityHolder.length; i++) {
            if (selectivityHolder[i] != null) {
                for (SelectivityHolder holder: selectivityHolder[i]) {
                    for (QualifierPhase phase:phases) {
                        if (holder.getPhase().equals(phase))
                            holders.add(holder); // Only add Phased Qualifiers
                    }
                }
            }
        }
        Collections.sort(holders);
        return computeSelectivity(totalSelectivity,holders);
    }

    public static double computeSelectivity(double selectivity, List<SelectivityHolder> holders) throws StandardException {
        for (int i = 0; i< holders.size();i++) {
            selectivity = computeSqrtLevel(selectivity,i,holders.get(i));
        }
        return selectivity;
    }

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

    private boolean addRangeQualifier(Predicate p,QualifierPhase phase) throws StandardException{
        DataValueDescriptor value=p.getCompareValue(baseTable);
        RelationalOperator relop=p.getRelop();
        int colNum = relop.getColumnOperand(baseTable).getColumnNumber();
        int relationalOperator = relop.getOperator();
        List<SelectivityHolder> columnHolder = getSelectivityListForColumn(colNum);
        OP_SWITCH: switch(relationalOperator){
            case RelationalOperator.EQUALS_RELOP:
                columnHolder.add(new RangeSelectivity(storeCost,value,value,true,true,colNum,phase));
                break;
            case RelationalOperator.NOT_EQUALS_RELOP:
                columnHolder.add(new NotEqualsSelectivity(storeCost,colNum,phase,value));
                break;
            case RelationalOperator.IS_NULL_RELOP:
                columnHolder.add(new NullSelectivity(storeCost,colNum,phase));
                break;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                columnHolder.add(new NotNullSelectivity(storeCost,colNum,phase));
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
                columnHolder.add(new RangeSelectivity(storeCost,value,null,true,true,colNum,phase));
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
                columnHolder.add(new RangeSelectivity(storeCost,value,null,true,false,colNum,phase));
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
                columnHolder.add(new RangeSelectivity(storeCost,null,value,true,true,colNum,phase));
                break;
            case RelationalOperator.LESS_THAN_RELOP:
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
                columnHolder.add(new RangeSelectivity(storeCost,null,value,true,false,colNum,phase));
                break;
            default:
                throw new RuntimeException("Unknown Qualifier Type");
         }
        return true;
    }


}
