package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.compiler.MethodBuilder;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.impl.sql.compile.*;


public class OrderByNode extends SingleChildResultSetNode {
	OrderByList		orderByList;
    @Override
    public boolean isParallelizable() {
        return true; //represented by a sort operation
    }
    
    

    /**
	 * Initializer for a OrderByNode.
	 *
	 * @param childResult	The child ResultSetNode
	 * @param orderByList	The order by list.
 	 * @param tableProperties	Properties list associated with the table
    *
	 * @exception StandardException		Thrown on error
	 */
	public void init(
						Object childResult,
						Object orderByList,
						Object tableProperties) throws StandardException {
		ResultSetNode child = (ResultSetNode) childResult;

		super.init(childResult, tableProperties);

		this.orderByList = (OrderByList) orderByList;

		ResultColumnList prRCList;

		/*
			We want our own resultColumns, which are virtual columns
			pointing to the child result's columns.

			We have to have the original object in the distinct node,
			and give the underlying project the copy.
		 */

		/* We get a shallow copy of the ResultColumnList and its 
		 * ResultColumns.  (Copy maintains ResultColumn.expression for now.)
		 */
		prRCList = child.getResultColumns().copyListAndObjects();
		resultColumns = child.getResultColumns();
		child.setResultColumns(prRCList);

		/* Replace ResultColumn.expression with new VirtualColumnNodes
		 * in the DistinctNode's RCL.  (VirtualColumnNodes include
		 * pointers to source ResultSetNode, this, and source ResultColumn.)
		 */
		resultColumns.genVirtualColumnNodes(this, prRCList);
	}


	/**
	 * Prints the sub-nodes of this object.  See QueryTreeNode.java for
	 * how tree printing is supposed to work.
	 *
	 * @param depth		The depth of this node in the tree
	 */

	public void printSubNodes(int depth)
	{
		if (SanityManager.DEBUG)
		{
			super.printSubNodes(depth);

			if (orderByList != null)
			{
				printLabel(depth, "orderByList: ");
				orderByList.treePrint(depth + 1);
			}
		}
	}
	@Override
	public ResultColumnDescriptor[] makeResultDescriptors() {
	    return childResult.makeResultDescriptors();
	}

	@Override
	public CostEstimate getFinalCostEstimate() throws StandardException{
		CostEstimate est = super.getFinalCostEstimate();
		CostEstimate base=est.getBase();
		if(base!=est){
			base.setRemoteCost(0d);
			est.setBase(null);
		}
		return est;
	}

	@Override
	public void generate(ActivationClassBuilder acb, MethodBuilder mb) throws StandardException {
		// Get the cost estimate for the child
		if (costEstimate == null) {			
			costEstimate = childResult.getFinalCostEstimate().cloneMe();
		}

	    orderByList.generate(acb, mb, childResult);

		// We need to take note of result set number if ORDER BY is used in a
		// subquery for the case where a PRN is inserted in top of the select's
		// PRN to project away a sort column that is not part of the select
		// list, e.g.
		//
		//     select * from (select i from t order by j desc) s
		//
		// If the resultSetNumber is not correctly set in our resultColumns,
		// code generation for the PRN above us will fail when calling
		// resultColumns.generateCore -> VCN.generateExpression, cf. the Sanity
		// assert in VCN.generateExpression on sourceResultSetNumber >= 0.
		resultSetNumber = orderByList.getResultSetNumber();
		resultColumns.setResultSetNumber(resultSetNumber);
	}
/*
	@Override
	public CostEstimate estimateCost(OptimizablePredicateList predList,
			ConglomerateDescriptor cd, CostEstimate outerCost,
			Optimizer optimizer, RowOrdering rowOrdering)
			throws StandardException {
		CostEstimate costEstimate = super.estimateCost(predList, cd, outerCost, optimizer, rowOrdering);
		costEstimate.setEstimatedCost(costEstimate.getEstimatedCost()+ ((double) costEstimate.getEstimatedRowCount()*SpliceConstants.optimizerWriteCost));
		return costEstimate;
	}



	@Override
	public CostEstimate getFinalCostEstimate() throws StandardException {
		CostEstimate costEstimate = super.getFinalCostEstimate();
		costEstimate.setEstimatedCost(costEstimate.getEstimatedCost()+ ((double) costEstimate.getEstimatedRowCount()*SpliceConstants.optimizerWriteCost));
		return costEstimate;
	}	
	*/
	
}
