package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
/**
 * 
 * TODO - Operation Execution with a countdown latch to manage parallization.
 * 
 * Probably needs to be backed by a zookeeper watcher.
 * 
 * @author johnleach
 *
 */
public class OperationExecution {
	protected List<OperationBranch> branches;
	protected CountDownLatch countDownLatch;
	public OperationExecution(OperationBranch branch) {
		branches = new ArrayList<OperationBranch>();
		branches.add(branch);		
	}

	public OperationExecution(List<OperationBranch> branches) {
		this.branches = branches;
		countDownLatch = new CountDownLatch(branches.size());
	}	
	
}
