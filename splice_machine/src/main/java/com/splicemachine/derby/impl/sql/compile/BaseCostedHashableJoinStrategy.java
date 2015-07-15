package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.impl.sql.compile.*;

import java.util.BitSet;

/**
 * Abstract class to provide convenience methods for costing joins.
 *
 * @author Scott Fines
 *         Date: 3/25/15
 */
public abstract class BaseCostedHashableJoinStrategy extends HashableJoinStrategy{

    protected double getTotalHeapSize(double outerHeapSize,
                                        double innerHeapSize,
                                        double innerRowCount,
                                        double outerRowCount,
                                        double totalOutputRows){
        double perRowHeapSize = outerHeapSize/outerRowCount;
        perRowHeapSize+=innerHeapSize/innerRowCount;
        return perRowHeapSize*totalOutputRows;
    }

    protected double getTotalRemoteCost(double outerRemoteCost,
                                        double innerRemoteCost,
                                        double outerRowCount,
                                        double innerRowCount,
                                        double totalOutputRows){
        double perRowRemoteCost = outerRemoteCost/outerRowCount;
        perRowRemoteCost+=innerRemoteCost/innerRowCount;
        return totalOutputRows*perRowRemoteCost;
    }

    protected double estimateJoinSelectivity(Optimizable innerTable,
                                             ConglomerateDescriptor cd,
                                             OptimizablePredicateList predList,
                                             double innerRowCount) throws StandardException{
        IndexRowGenerator irg = null;
        if (cd != null) {
            irg = cd.getIndexDescriptor();
        }
        if(cd == null || irg==null||irg.getIndexDescriptor()==null){
            return estimateNonKeyedSelectivity(innerTable,cd,predList);
        }else{
            return estimateKeyedSelectivity(innerTable,cd,predList,irg,innerRowCount);
        }
    }

    private double estimateKeyedSelectivity(Optimizable innerTable,
                                            ConglomerateDescriptor cd,
                                            OptimizablePredicateList predList,
                                            IndexRowGenerator irg,
                                            double innerRowCount) throws StandardException{
        BitSet setKeyColumns = new BitSet(irg.numberOfOrderedColumns());
        for(int i=0;i<predList.size();i++){
            Predicate pred = (Predicate)predList.getOptPredicate(i);
            if(!pred.isJoinPredicate()) continue;
            //we can only check equals predicates to know what the selectivity will be precisely
            RelationalOperator relop=pred.getRelop();
            if(relop.getOperator()==RelationalOperator.EQUALS_RELOP){
                ColumnReference column=relop.getColumnOperand(innerTable);
                int keyColPos=irg.getKeyColumnPosition(column.getColumnNumber());
                if(keyColPos>0){
                    setKeyColumns.set(keyColPos-1);
                }
            }
        }

        if(setKeyColumns.cardinality()==irg.numberOfOrderedColumns()){
            /*
             * If we have a equality predicate on every join column, then we know that we will
             * basically perform a lookup for every row, so we should reduce innerCost's rowCount
             * to 1 with our selectivity. Because we do this, we do(!includeStart || !includeStop)) return 0l; //empty interval has no data so don't bother adding in any additional
             * selectivity
             */
            return 1d/innerRowCount;
        }else{
            /*
             * We don't have a complete set of predicates on the key columns of the inner table,
             * so we really don't have much choice besides going with the base selectivity
             */
            return estimateNonKeyedSelectivity(innerTable,cd,predList);
        }
    }

    private double estimateNonKeyedSelectivity(Optimizable innerTable,
                                               ConglomerateDescriptor cd,
                                               OptimizablePredicateList predList) throws StandardException{
        double selectivity = 1.0d;
//        StoreCostController scc= ((FromTable)innerTable).getCompilerContext().getStoreCostController(cd);
        for(int i=0;i<predList.size();i++){
            Predicate p = (Predicate)predList.getOptPredicate(i);
            if(!p.isJoinPredicate()) continue;
            selectivity*=p.selectivity(innerTable,cd);
//            ColumnReference columnOperand=p.getRelop().getColumnOperand(innerTable);
//            if(columnOperand==null) continue; //ignore predicates which are not on the join table
//            int colNumber = columnOperand.getColumnNumber();
//            double innerCard = scc.cardinalityFraction(colNumber);
//            /*
//             * We have the inner cardinality, which tells us the number of rows which
//             */
//            selectivity*=innerCard;
        }
        return selectivity;
    }

}
