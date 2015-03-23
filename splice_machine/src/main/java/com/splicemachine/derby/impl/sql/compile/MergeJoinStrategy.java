package com.splicemachine.derby.impl.sql.compile;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;

import com.google.common.base.Preconditions;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.AccessPath;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.sql.compile.JoinStrategy;
import com.splicemachine.db.iapi.sql.compile.Optimizable;
import com.splicemachine.db.iapi.sql.compile.OptimizablePredicateList;
import com.splicemachine.db.iapi.sql.compile.Optimizer;
import com.splicemachine.db.iapi.sql.compile.RowOrdering;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.IndexRowGenerator;
import com.splicemachine.db.iapi.util.JBitSet;
import com.splicemachine.db.impl.sql.compile.BaseTableNumbersVisitor;
import com.splicemachine.db.impl.sql.compile.BinaryRelationalOperatorNode;
import com.splicemachine.db.impl.sql.compile.FromBaseTable;
import com.splicemachine.db.impl.sql.compile.FromTable;
import com.splicemachine.db.impl.sql.compile.HashableJoinStrategy;
import com.splicemachine.db.impl.sql.compile.Predicate;
import com.splicemachine.db.impl.sql.compile.RelationalOperator;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.utils.SpliceLogUtils;

public class MergeJoinStrategy extends HashableJoinStrategy {
    private static final Logger LOG = Logger.getLogger(MergeJoinStrategy.class);

    public MergeJoinStrategy() {
    }

    /**
     * @see JoinStrategy#getName
     */
    public String getName() {
        return "MERGE";
    }

    /**
     * @see JoinStrategy#resultSetMethodName
     */
	@Override
    public String resultSetMethodName(boolean bulkFetch, boolean multiprobe) {
        if (bulkFetch)
            return "getBulkTableScanResultSet"; 
        else if (multiprobe)
            return "getMultiProbeTableScanResultSet";
        else
            return "getTableScanResultSet";
    }

    /**
     * @see JoinStrategy#joinResultSetMethodName
     */
	@Override
    public String joinResultSetMethodName() {
        return "getMergeJoinResultSet";
    }

	/** @see JoinStrategy#multiplyBaseCostByOuterRows */
	public boolean multiplyBaseCostByOuterRows() {
		return true;
	}

    /**
     * @see JoinStrategy#halfOuterJoinResultSetMethodName
     */
	@Override
    public String halfOuterJoinResultSetMethodName() {
        return "getMergeLeftOuterJoinResultSet";
    }

    /** @see com.splicemachine.db.iapi.sql.compile.JoinStrategy#estimateCost */
    @Override
    public void estimateCost(Optimizable innerTable,
                             OptimizablePredicateList predList,
                             ConglomerateDescriptor cd,
                             CostEstimate outerCost,
                             Optimizer optimizer,
                             CostEstimate costEstimate) {
			SpliceLogUtils.trace(LOG, "estimateCost innerTable=%s,predList=%s,conglomerateDescriptor=%s,outerCost=%s,optimizer=%s,costEstimate=%s",innerTable,predList,cd,outerCost,optimizer,costEstimate);
    }

	@Override
	public boolean feasible(Optimizable innerTable,OptimizablePredicateList predList, Optimizer optimizer) throws StandardException {
//		if (CostUtils.isThisBaseTable(optimizer)) {
//			SpliceLogUtils.trace(LOG, "not feasible base table innerTable=%s, predList=%s, optimizer=%s",innerTable,predList,optimizer);
//			return false;
//		}
		SpliceLevel2OptimizerImpl opt = (SpliceLevel2OptimizerImpl) optimizer;

        /* Currently MergeJoin does not work with a right side IndexRowToBaseRowOperation */
        if(JoinStrategyUtil.isNonCoveringIndex(innerTable)) {
            return false;
        }
        CostEstimate cost = innerTable.getBestAccessPath().getCostEstimate();
        if(cost.getRowOrdering()==null) return false;

//		CostEstimate outerCost;
//		if (opt.joinPosition == 0) {
//			outerCost = opt.outermostCostEstimate;
//		} else {
//			// You cannot sit on top of a MergeSort if you are a Merge Join, it breaks your sorting...
//			for (int i = 0; i<=opt.joinPosition-1;i++) {
//				AccessPath bestAccessPathCheck = opt.optimizableList.getOptimizable(opt.proposedJoinOrder[opt.joinPosition - 1]).getBestAccessPath();
//				if (bestAccessPathCheck.getJoinStrategy().equals(optimizer.getJoinStrategy(1)))
//					return false;
//			}
//			/*
//			** NOTE: This is somewhat problematic.  We assume here that the
//			** outer cost from the best access path for the outer table
//			** is OK to use even when costing the sort avoidance path for
//			** the inner table.  This is probably OK, since all we use
//			** from the outer cost is the row count.
//			*/
//			outerCost = opt.optimizableList.getOptimizable(opt.proposedJoinOrder[opt.joinPosition - 1]).getBestAccessPath().getCostEstimate();
//		}
//		if (outerCost.getRowOrdering() == null) // not feasible
//			return false;
        boolean hashFeasible = hashFeasible(cost.getRowOrdering(),innerTable, predList, optimizer);
        SpliceLogUtils.trace(LOG,
                "feasible innerTable=%s, predList=%s, optimizer=%s, hashFeasible=%s, outerCost=%s",
                innerTable,predList,optimizer,hashFeasible, cost);
		return hashFeasible;
	}

	/**
	 *
	 * Right Side Cost + NetworkCost
	 *
	 */
	@Override
	public void estimateCost(OptimizablePredicateList predList,CostEstimate outerCost,CostEstimate innerCost) {
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate outerCost=%s, innerFullKeyCost=%s",outerCost, innerCost);
		// InnerCost does not change
		SpliceCostEstimateImpl inner = (SpliceCostEstimateImpl) innerCost;
		inner.setBase(innerCost.cloneMe());
		SpliceCostEstimateImpl outer = (SpliceCostEstimateImpl) outerCost;
        Preconditions.checkState(outer.numberOfRegions > 0);
        long rowCount = Math.max(inner.getEstimatedRowCount(), outer.getEstimatedRowCount());
		double joinCost = inner.getEstimatedRowCount()*SpliceConstants.remoteRead/outer.numberOfRegions;
		inner.setCost(joinCost+inner.cost+outer.cost, rowCount, rowCount);
		inner.setNumberOfRegions(outer.numberOfRegions);
		inner.setRowOrdering(outer.rowOrdering);
		SpliceLogUtils.trace(LOG, "rightResultSetCostEstimate computed cost innerCost=%s",innerCost);
	};

	 public boolean hashFeasible(RowOrdering rowOrdering, Optimizable innerTable,OptimizablePredicateList predList,Optimizer optimizer) throws StandardException {
        //commented out because it's annoying -SF-
        int[] hashKeyColumns = null;
        ConglomerateDescriptor cd = null;

		/* If the innerTable is a VTI, then we must check to see if there are any
		 * join columns in the VTI's parameters.  If so, then hash join is not feasible.
		 */
        if (! innerTable.isMaterializable()) {
            optimizer.trace(Optimizer.HJ_SKIP_NOT_MATERIALIZABLE, 0, 0, 0.0,null);
            return false;
        }

		/* Don't consider hash join on the target table of an update/delete.
		 * RESOLVE - this is a temporary restriction.  Problem is that we
		 * do not put RIDs into the row in the hash table when scanning
		 * the heap and we need them for a target table.
		 */
        if (innerTable.isTargetTable()) {
            return false;
        }

		/* If the predicate given by the user _directly_ references
		 * any of the base tables _beneath_ this node, then we
		 * cannot safely use the predicate for a hash because the
		 * predicate correlates two nodes at different nesting levels.
		 * If we did a hash join in this case, materialization of
		 * innerTable could lead to incorrect results--and in particular,
		 * results that are missing rows.  We can check for this by
		 * looking at the predicates' reference maps, which are set based
		 * on the initial query (as part of pre-processing).  Note that
		 * by the time we get here, it's possible that a predicate's
		 * reference map holds table numbers that do not agree with the
		 * table numbers of the column references used by the predicate.
		 * That's okay--this occurs as a result of "remapping" predicates
		 * that have been pushed down the query tree.  And in fact
		 * it's a good thing because, by looking at the column reference's
		 * own table numbers instead of the predicate's referenced map,
		 * we are more readily able to find equijoin predicates that
		 * we otherwise would not have found.
		 *
		 * Note: do not perform this check if innerTable is a FromBaseTable
		 * because a base table does not have a "subtree" to speak of.
		 */
        if ((predList != null) && (predList.size() > 0) && !(innerTable instanceof FromBaseTable)) {
            FromTable ft = (FromTable)innerTable;
            // First get a list of all of the base tables in the subtree
            // below innerTable.
            JBitSet tNums = new JBitSet(ft.getReferencedTableMap().size());
            BaseTableNumbersVisitor btnVis = new BaseTableNumbersVisitor(tNums);
            ft.accept(btnVis);

            // Now get a list of all table numbers referenced by the
            // join predicates that we'll be searching.
            JBitSet pNums = new JBitSet(tNums.size());
            Predicate pred = null;
            for (int i = 0; i < predList.size(); i++) {
                pred = (Predicate)predList.getOptPredicate(i);
                pred.isJoinPredicate();
                if (pred.isJoinPredicate())
                    pNums.or(pred.getReferencedSet());
            }

            // If tNums and pNums have anything in common, then at
            // least one predicate in the list refers directly to
            // a base table beneath this node (as opposed to referring
            // just to this node), which means it's not safe to do a
            // hash join.
            tNums.and(pNums);
            if (tNums.getFirstSetBit() != -1) {
                return false;
            }
        }
        IndexRowGenerator irg = null;
        if (innerTable.isBaseTable()) {
			/* Must have an equijoin on a column in the conglomerate */
            cd = innerTable.getCurrentAccessPath().getConglomerateDescriptor();
            if (cd.getConglomerateNumber() < 1184)
            	return false; // temporary: system tables cannot support merge, argh. JL
        }
        if (cd == null) {
        	return false;
        }
        else if (cd.isIndex())
        	irg = cd.getIndexDescriptor();
        else {
    		if (cd != null && !cd.isIndex() && !cd.isConstraint()) {
    			   ConglomerateDescriptorList cdl = innerTable.getTableDescriptor().getConglomerateDescriptorList();
    			   for(int index=0;index< cdl.size();index++){
    		            ConglomerateDescriptor primaryKeyCheck = (ConglomerateDescriptor) cdl.get(index);
    		            IndexRowGenerator indexDec = primaryKeyCheck.getIndexDescriptor();
    		            if(indexDec!=null){
    		                String indexType = indexDec.indexType();
    		                if(indexType!=null && indexType.contains("PRIMARY")) {
    		                	irg = indexDec;
    		                	break;
    		                }
    		            }
    		        }
    			}
        }

        if (irg == null) // Not Sorted...
        	return false;
        
		/* Look for equijoins in the predicate list */
        hashKeyColumns = findHashKeyColumns(innerTable,cd,predList);

        if (SanityManager.DEBUG) {
            if (hashKeyColumns == null) {
                optimizer.trace(Optimizer.HJ_SKIP_NO_JOIN_COLUMNS, 0, 0, 0.0, null);
            }
            else {
                optimizer.trace(Optimizer.HJ_HASH_KEY_COLUMNS, 0, 0, 0.0, hashKeyColumns);
            }
        }

        if (hashKeyColumns == null) {
            return false;
        }



        return isMergeable(innerTable, predList,irg,(SpliceRowOrderingImpl) rowOrdering);
    }

	 public boolean isMergeable(Optimizable innerTable, OptimizablePredicateList predicateList, IndexRowGenerator irg, SpliceRowOrderingImpl rowOrdering) throws StandardException {
		 int[] baseColumnPositionsRightSide = irg.baseColumnPositions();
		 Vector leftSideOrdering = rowOrdering.getVector();
		 int size = predicateList.size();
		 List<BinaryRelationalOperatorNode> joinOperators = new ArrayList<BinaryRelationalOperatorNode>();
		 for (int i = 0; i<size; i++) {
			 Predicate pred = (Predicate) predicateList.getOptPredicate(i);
			 if (pred.isJoinPredicate()) {
				 RelationalOperator relop = pred.getRelop();
				 if (relop != null && relop instanceof BinaryRelationalOperatorNode) {
					 joinOperators.add((BinaryRelationalOperatorNode) relop);
				 } else {
					 return false; // a join that is not what we are looking for :)
				 }
			 }
		 }
		 int orderedSize = Math.min(leftSideOrdering.size(), baseColumnPositionsRightSide.length);
		 int removalSize = joinOperators.size();
		 int testSize = Math.min(orderedSize, removalSize);
		 for (int j = 0; j< testSize; j++) {
			 ColumnOrdering co = (ColumnOrdering) leftSideOrdering.get(j);
			 if ( (co.direction() == RowOrdering.ASCENDING && irg.isAscending(j+1)) ||
					 co.direction() == RowOrdering.DESCENDING && irg.isDescending(j+1)) {
				 for (BinaryRelationalOperatorNode brelop: joinOperators) {
					  int[] colTab = co.get(0);
					 if (brelop.isOrderedQualifier(colTab[0], colTab[1], innerTable.getTableNumber(), baseColumnPositionsRightSide[j])) {
						 removalSize--;
					 } else {
					 }
				 }

			 } else {
				 break;
			 }
		 }
		 if (removalSize == 0 ) {
			 return true;
		 }
		 return false;
	 }

}