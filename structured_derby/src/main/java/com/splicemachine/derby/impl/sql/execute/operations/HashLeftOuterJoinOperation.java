package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.GeneratedMethod;
import org.apache.derby.iapi.sql.Activation;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

public class HashLeftOuterJoinOperation extends NestedLoopLeftOuterJoinOperation {
	private static Logger LOG = Logger.getLogger(HashLeftOuterJoinOperation.class);
	static {
		nodeTypes = new ArrayList<NodeType>();
		nodeTypes.add(NodeType.MAP);	
		nodeTypes.add(NodeType.SCROLL);
	}
	
	public HashLeftOuterJoinOperation() {
		super();
	}
	
	public HashLeftOuterJoinOperation (
			NoPutResultSet leftResultSet,
			int leftNumCols,
			NoPutResultSet rightResultSet,
			int rightNumCols,
			Activation activation,
			GeneratedMethod restriction,
			int resultSetNumber,
			GeneratedMethod emptyRowFun,
			boolean wasRightOuterJoin,
		    boolean oneRowRightSide,
		    boolean notExistsRightSide,
			double optimizerEstimatedRowCount,
			double optimizerEstimatedCost,
			String userSuppliedOptimizerOverrides) throws StandardException {
		super(leftResultSet, leftNumCols, rightResultSet, rightNumCols, activation, restriction, resultSetNumber, emptyRowFun, wasRightOuterJoin,
				oneRowRightSide, notExistsRightSide, optimizerEstimatedRowCount, optimizerEstimatedCost, userSuppliedOptimizerOverrides);
		this.isHash = true;
		SpliceLogUtils.trace(LOG, "instantiate");
	}
}
