package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.impl.sql.compile.ColumnReference;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.RelationalOperator;

import java.util.BitSet;

/**
 * Abstract class to provide convenience methods for costing joins.
 *
 * @author Scott Fines
 *         Date: 3/25/15
 */
public abstract class BaseCostedHashableJoinStrategy extends HashableJoinStrategy{

    protected double getTotalHeapSize(CostEstimate outerCost,CostEstimate innerCost,double totalOutputRows){
        double perRowHeapSize = outerCost.getEstimatedHeapSize()/outerCost.rowCount();
        perRowHeapSize+=innerCost.getEstimatedHeapSize()/innerCost.rowCount();
        return perRowHeapSize*totalOutputRows;
    }

    protected double getTotalRemoteCost(CostEstimate outerCost,CostEstimate innerCost,double totalOutputRows){
        double perRowRemoteCost = outerCost.remoteCost()/outerCost.rowCount();
        perRowRemoteCost+=innerCost.remoteCost()/innerCost.rowCount();
        return totalOutputRows*perRowRemoteCost;
    }

    protected double estimateJoinSelectivity(Optimizable innerTable,
                                           ConglomerateDescriptor cd,
                                           OptimizablePredicateList predList,
                                           CostEstimate outerCost,
                                           CostEstimate innerCost) throws StandardException{
        IndexRowGenerator irg=cd.getIndexDescriptor();
        if(irg==null||irg.getIndexDescriptor()==null){
            return estimateNonKeyedSelectivity(innerTable,predList);
        }else{
            return estimateKeyedSelectivity(innerTable,predList,irg,innerCost);
        }
    }

    private double estimateKeyedSelectivity(Optimizable innerTable,
                                            OptimizablePredicateList predList,
                                            IndexRowGenerator irg,
                                            CostEstimate innerCost) throws StandardException{
        BitSet setKeyColumns = new BitSet(irg.numberOfOrderedColumns());
        for(int i=0;i<predList.size();i++){
            Predicate pred = (Predicate)predList.getOptPredicate(i);
            if(!pred.isJoinPredicate()) continue;
            //we can only check equals predicates to know what the selectivity will be precisely
            if(pred.getRelop().getOperator()==RelationalOperator.EQUALS_RELOP){
                ColumnReference column=pred.getRelop().getColumnOperand(innerTable);
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
             * to 1 with our selectivity. Because we do this, we don't bother adding in any additional
             * selectivity
             */
            return 1d/innerCost.rowCount();
        }else{
            /*
             * We don't have a complete set of predicates on the key columns of the inner table,
             * so we really don't have much choice besides going with the base selectivity
             */
            return estimateNonKeyedSelectivity(innerTable,predList);
        }
    }

    private double estimateNonKeyedSelectivity(Optimizable innerTable,OptimizablePredicateList predList) throws StandardException{
        double selectivity = 1.0d;
        for(int i=0;i<predList.size();i++){
            Predicate p = (Predicate)predList.getOptPredicate(i);
            if(!p.isJoinPredicate()) continue;
            selectivity*=p.selectivity(innerTable); //TODO -sf- improve this
        }
        return selectivity;
    }
}
