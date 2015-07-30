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
 * @author Scott Fines
 *         Date: 5/15/15
 */
public class ScanCostFunction{
    private final Optimizable baseTable;
    private final CostEstimate scanCost;
    private final StoreCostController storeCost;

    private double nonQualifierSelectivity = 1.0d; //applied after index lookups
    private double extraQualifierSelectivity = 1d; //applied before index lookups
    private final BitSet scanColumns; //the columns that we are scanning
    private final BitSet lookupColumns; //the columns we are performing a lookup for

    private List<RangeQualifier> firstKeyQualifiers;
    private RangeQualifier[] keyQualifiers; //qualifiers which apply to key columns
    private List<RangeQualifier>[] nonKeyQualifiers; //qualifiers which apply to non-key columns
    private final int[] keyColumns;
//    private final int scanType;
    private final long baseRowCount;
    private final boolean forUpdate;
    private final DataValueDescriptor[] rowTemplate;
    private final int startOperator;
    private final int stopOperator;

    private transient boolean baseCostComputed;
    private transient boolean lookupCostComputed;
    private transient boolean outputCostComputed;

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
                            int stopOperator){
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
        int nonKeyLen = rowTemplate.length; //always non-null, but not always right, if there's a mapping
        if(scanColumns!=null)
            nonKeyLen = Math.max(scanColumns.length(),nonKeyLen); //choose the larger of the two, to make room
        //noinspection unchecked
        this.nonKeyQualifiers = new List[nonKeyLen];
        this.rowTemplate = rowTemplate;
        if(keyColumns!=null && keyColumns.length>0)
            keyQualifiers = new RangeQualifier[keyColumns.length];
    }


    void addPredicate(Predicate p) throws StandardException{
        baseCostComputed = false;
        lookupCostComputed = false;
        outputCostComputed = false;
        /*
         * Predicates can be broken down into three categories:
         *
         * 1. Qualifier predicates --> These are predicates which compare to a constant
         * node (or can be constructed to compare to a constant node). If the predicate can
         * be serialized and applied to the byte-encoded version of the data, then it's a qualifier
         * 2. NonQualifier predicates --> These are predicates which cannot be applied to the
         * byte encoding of the data. Instead, a ProjectRestrict has to be used to apply the restriction
         * 3. InList predicates --> these are predicates which are equivalent to an IN clause (either IN or
         * OR).  If an InList Predicate can be applied to the first column of a keyed conglomerate, then
         * it will also appear to be a qualifier, but will *not* be considered a start/stop key, and will
         * *not* show up as "constant". In all other cases, the InList predicate will look like a non-qualifier
         * predicate.
         *
         * It's also possible that an InList contains non-constant elements (e.g. subqueries). In that case,
         * we have to consider the operation as a non-qualifier predicate, because it won't be converted to a
         * MultiProbe scan in the future.
         *
         * Any InList clause which does not occur on the *FIRST KEYED COLUMN* should always be treated as a
         * non-qualifier predicate
         */
        if(isMultiProbeQualifier(p)){
            //add an EQUALS RangePredicate for each ConstantNode in the list
            InListOperatorNode sourceInList=p.getSourceInList();
            ValueNodeList rightOperandList=sourceInList.getRightOperandList();
            for(Object o: rightOperandList){
                ConstantNode cn = (ConstantNode)o;
                DataValueDescriptor value=cn.getValue();
                List<RangeQualifier> keyQualifier=firstKeyQualifiers;
                if(keyQualifier ==null){
                    keyQualifier=firstKeyQualifiers = new LinkedList<>();
                }
                keyQualifier.add(new RangeQualifier(value,value,true,true));
            }
        }  else if(p.isQualifier()){
            boolean knownConstant = p.compareWithKnownConstant(baseTable,true);
            if(knownConstant){
                ColumnReference columnOperand=getRelop(p).getColumnOperand(baseTable);
                if(columnOperand==null){
                    nonQualifierSelectivity*=p.selectivity(baseTable);
                    return;
                }
                int colNum=columnOperand.getColumnNumber();
                boolean start = p.isStartKey();
                boolean stop = p.isStopKey();
                if(start || stop){
                    if(keyColumns!=null){
                        for(int i=0;i<keyColumns.length;i++){
                            if(keyColumns[i]==colNum){
                                addKeyQualifier(p,i);
                            }
                        }
                    }
                }else{
                    addNonKeyQualifier(p,colNum);
                }
            }else
                nonQualifierSelectivity*=p.selectivity(baseTable);
        } else if(!p.isJoinPredicate())
            nonQualifierSelectivity*=p.selectivity(baseTable);
    }

    void computeBaseScanCost() throws StandardException{
        if(baseCostComputed) return; //already computed, don't do it again
        /*
         * We create a key for scanning, in order to compute our base costs
         */
        if(firstKeyQualifiers!=null){
            /*
             * We are doing a multi-probe scan, so move any additional key qualifiers
             * for the first entry to the non-key qualifiers list.
             */
            RangeQualifier firstQual = keyQualifiers[0];
            if(firstQual!=null){
                List<RangeQualifier> nonKeyQualifier=nonKeyQualifiers[keyColumns[0]];
                if(nonKeyQualifier==null)
                    nonKeyQualifiers[keyColumns[0]] = Collections.singletonList(firstQual);
                else
                    nonKeyQualifier.add(firstQual);
                keyQualifiers[0] = null;
            }
        }

        //get the first gap in key range
        int startKeyLen = firstKeyQualifiers!=null? 1: 0;
        int stopKeyLen = firstKeyQualifiers!=null? 1: 0;
        boolean startGood = true, stopGood = true;
        if(keyQualifiers!=null){
            for(int i=startKeyLen;i<keyQualifiers.length&&(startGood || stopGood);i++){
                RangeQualifier keyQualifier=keyQualifiers[i];
                if(keyQualifier==null){
                    break;
                }
                if(keyQualifier.start==null){
                    startGood = false;
                }else
                    startKeyLen++;
                if(keyQualifier.stop==null){
                    stopGood = false;
                }else
                    stopKeyLen++;
            }
        }
        DataValueDescriptor[] startKeys = new DataValueDescriptor[startKeyLen];
        DataValueDescriptor[] stopKeys = new DataValueDescriptor[stopKeyLen];
        for(int i=firstKeyQualifiers!=null?1:0;i<startKeyLen;i++){
            //this is fine, because the loop won't happen if keyQualifiers is null(startKeyLen will be 0 or 1)
            @SuppressWarnings("ConstantConditions") RangeQualifier keyQual = keyQualifiers[i];
            startKeys[i] = keyQual.start;
            if(i<stopKeyLen)
                stopKeys[i] = keyQual.stop;
        }
        for(int i=startKeyLen;i<stopKeyLen;i++){
            stopKeys[i] = keyQualifiers[i].stop;
        }

        List<DataValueDescriptor> probeStartKeys = null;
        if(firstKeyQualifiers!=null){
            probeStartKeys = new ArrayList<>(firstKeyQualifiers.size());
            for(RangeQualifier probe:firstKeyQualifiers){
                probeStartKeys.add(probe.start);
            }
        }
        storeCost.getScanCost(baseRowCount,
                forUpdate,
                scanColumns,
                rowTemplate,
                probeStartKeys,
                startKeys,
                startOperator,
                stopKeys,
                stopOperator,
                scanCost);

        baseCostComputed = true;
    }

    void computeLookupCost() throws StandardException{
        if(lookupCostComputed) return; //nothing to do
        this.computeBaseScanCost();
        lookupCostComputed = true;

        /*
         * Reduce the number of rows which are fed to the index lookup by the
         * extra qualifier selectivity
         */
        computeExtraQualifierSelectivity();
        if(lookupColumns==null){
            nonQualifierSelectivity*=extraQualifierSelectivity;
            return;
        }
        double qualifiedRows = extraQualifierSelectivity*scanCost.rowCount();
        double qualifiedHeap = extraQualifierSelectivity*scanCost.getEstimatedHeapSize();
        double qualifiedRemoteCost = extraQualifierSelectivity*scanCost.remoteCost();
        scanCost.setRowCount(qualifiedRows);
        scanCost.setRemoteCost(qualifiedRemoteCost);
        scanCost.setEstimatedHeapSize((long)qualifiedHeap);

        /*
         * Multiply all the rows by the cost to perform the lookup
         */
        storeCost.getFetchFromRowLocationCost(lookupColumns,0,scanCost);
    }


    void computeOutputCost() throws StandardException{
        if(outputCostComputed) return; //nothing to do
        computeLookupCost();
        /*
         * The output rows are reduced by the non-qualifier predicates, but those predicates
         * aren't applied until after the index lookups, so we apply them here.
         */
        double outputRows = nonQualifierSelectivity*scanCost.rowCount();
        double outputHeap = nonQualifierSelectivity*scanCost.getEstimatedHeapSize();
        double outputRemoteCost = nonQualifierSelectivity*scanCost.remoteCost();
        scanCost.setRowCount(outputRows);
        scanCost.setEstimatedHeapSize((long)outputHeap);
        scanCost.setRemoteCost(outputRemoteCost);
        outputCostComputed = true;
    }

    CostEstimate getScanCost() throws StandardException{
        computeOutputCost(); //ensure that the output cost is computed
        return scanCost;
    }

    /* ********************************************************************************/

    private void computeExtraQualifierSelectivity(){
        double eQuS = extraQualifierSelectivity;
        int colNum = 0;
        for(List<RangeQualifier> nonKeyQualifier: nonKeyQualifiers){
            colNum++; //indexed from 1
            if(nonKeyQualifier==null) continue; //skip null elements
            for(RangeQualifier qual:nonKeyQualifier){
                eQuS *= qual.selectivity(colNum);
            }
        }
        extraQualifierSelectivity = eQuS;
    }

    private void addKeyQualifier(Predicate p,int keyPos) throws StandardException{
        RangeQualifier keyQual = keyQualifiers[keyPos];
        if(keyQual==null)
            keyQual = keyQualifiers[keyPos] = new RangeQualifier();

        int colNum=keyColumns[keyPos];
        if(keyQual.start!=null && keyQual.stop!=null){
            //we have filled this position, so we are redundant
            addNonKeyQualifier(p,colNum);
            return;
        }

        DataValueDescriptor compareValue=p.getCompareValue(baseTable);
        RelationalOperator relop=getRelop(p);
        int relationalOperator = relop.getOperator();

        switch(relationalOperator){
            case RelationalOperator.EQUALS_RELOP:
                keyQual.start=compareValue;
                keyQual.stop=compareValue;
                keyQual.includeStart=keyQual.includeStop=true;
                break;
            case RelationalOperator.NOT_EQUALS_RELOP:
                double sel=1-storeCost.getSelectivity(colNum,compareValue,true,compareValue,true);
                extraQualifierSelectivity*=sel;
                break;
            case RelationalOperator.GREATER_EQUALS_RELOP:
                keyQual.start=compareValue;
                keyQual.includeStart=true;
                break;
            case RelationalOperator.GREATER_THAN_RELOP:
                keyQual.start=compareValue;
                keyQual.includeStart=false;
                break;
            case RelationalOperator.LESS_EQUALS_RELOP:
                keyQual.stop=compareValue;
                keyQual.includeStop=true;
                break;
            case RelationalOperator.LESS_THAN_RELOP:
                keyQual.stop=compareValue;
                keyQual.includeStop=false;
                break;
            case RelationalOperator.IS_NULL_RELOP:
                extraQualifierSelectivity*=storeCost.nullSelectivity(colNum);
                break;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                extraQualifierSelectivity*=(1-storeCost.nullSelectivity(colNum));
                break;
            default:
                /*
                 * This should never happen, but just in case we apply this to the non-qualifier
                 * selectivity
                 */
                nonQualifierSelectivity*=p.selectivity(baseTable);
        }
    }

    private RelationalOperator getRelop(Predicate p){
        RelationalOperator relop=p.getRelop();
        assert relop!=null: "Programmer error! unexpected null Relational Operator";
        return relop;
    }

    private void addNonKeyQualifier(Predicate p,int colNum) throws StandardException{
        List<RangeQualifier> quals = nonKeyQualifiers[colNum-1];
        if(quals==null){
            quals = nonKeyQualifiers[colNum-1] = new LinkedList<>();
        }
        DataValueDescriptor compareValue=p.getCompareValue(baseTable);
        RelationalOperator relop=getRelop(p);
        int relationalOperator = relop.getOperator();
        if(!addRangeQualifier(quals,compareValue,relationalOperator,colNum)){
            nonQualifierSelectivity*=p.selectivity(baseTable);
        }
    }

    private boolean addRangeQualifier(List<RangeQualifier> qualifiers,
                                      DataValueDescriptor values,
                                      int relationalOperator,
                                      int colNum) throws StandardException{
        OP_SWITCH: switch(relationalOperator){
            case RelationalOperator.EQUALS_RELOP:
                qualifiers.add(new RangeQualifier(values,values,true,true));
                break;
            case RelationalOperator.NOT_EQUALS_RELOP:
                double sel = 1-storeCost.getSelectivity(colNum,values,true,values,false);
                extraQualifierSelectivity *=sel;
                break;
            case RelationalOperator.GREATER_EQUALS_RELOP:
                for(RangeQualifier rq: qualifiers){
                    if(rq.start==null){
                        rq.start = values;
                        rq.includeStart = true;
                        break OP_SWITCH;
                    }
                }
                qualifiers.add(new RangeQualifier(values,null,true,true));
                break;
            case RelationalOperator.GREATER_THAN_RELOP:
                for(RangeQualifier rq: qualifiers){
                    if(rq.start==null){
                        rq.start = values;
                        rq.includeStart = false;
                        break OP_SWITCH;
                    }
                }
                qualifiers.add(new RangeQualifier(values,null,true,false));
                break;
            case RelationalOperator.LESS_EQUALS_RELOP:
                for(RangeQualifier rq: qualifiers){
                    if(rq.stop==null){
                        rq.stop = values;
                        rq.includeStop = true;
                        break OP_SWITCH;
                    }
                }
                qualifiers.add(new RangeQualifier(null,values,true,true));
                break;
            case RelationalOperator.LESS_THAN_RELOP:
                for(RangeQualifier rq: qualifiers){
                    if(rq.stop==null){
                        rq.stop = values;
                        rq.includeStop = true;
                        break OP_SWITCH;
                    }
                }
                qualifiers.add(new RangeQualifier(null,values,true,false));
                break;
            case RelationalOperator.IS_NULL_RELOP:
                extraQualifierSelectivity*= storeCost.nullSelectivity(colNum);
                break;
            case RelationalOperator.IS_NOT_NULL_RELOP:
                extraQualifierSelectivity*= (1-storeCost.nullSelectivity(colNum));
                break;
            default:
                /*
                 * This should never happen, but just in case we apply this to the non-qualifier
                 * selectivity
                 */
                return false;
        }
        return true;
    }

    private boolean isMultiProbeQualifier(Predicate p){
        /*
         * Returns true if the predicate is a multi-probe qualifier.
         *
         * A Multi-probe qualifier satisfies 3 criteria:
         * 1. It has an IN clause
         * 2. It is applied against the first keyed column
         * 3. There are only constant nodes in the IN list
         */
        if(keyColumns==null) return false; //can't be a MPQ if there are no keyed columns
        InListOperatorNode sourceInList=p.getSourceInList();
        if(sourceInList==null) return false; //not a multi-probe predicate
        ValueNode lo = sourceInList.getLeftOperand();
        //if it doesn't refer to a column, then it can't be a qualifier
        if(!(lo instanceof ColumnReference)) return false;
        ColumnReference colRef = (ColumnReference)lo;
        int colNum = colRef.getColumnNumber();
        if(keyColumns[0]!=colNum) return false; //doesn't point to the first keyed column

        ValueNodeList rightOperandList=sourceInList.getRightOperandList();
        for(Object o:rightOperandList){
            if(!(o instanceof ConstantNode)) return false; //not all constants in the IN list
        }
        return true;
    }

    private class RangeQualifier{
        DataValueDescriptor start;
        boolean includeStart;
        DataValueDescriptor stop;
        boolean includeStop;

        public RangeQualifier(DataValueDescriptor start, DataValueDescriptor stop,boolean includeStart, boolean includeStop){
            this.start = start;
            this.stop = stop;
            this.includeStart = includeStart;
            this.includeStop = includeStop;
        }

        public RangeQualifier(){
        }

        public double selectivity(int colNum){
            return storeCost.getSelectivity(colNum,start,includeStart,stop,includeStop);
        }
    }

    static class Builder{

    }

}
