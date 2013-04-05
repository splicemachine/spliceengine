package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.derby.iapi.sql.execute.operations.IOperationTree;
import com.splicemachine.utils.SpliceLogUtils;

public class OperationTree implements IOperationTree {
	private static Logger LOG = Logger.getLogger(OperationTree.class);
	public enum OperationTreeStatus {
		CREATED, WAITING, ACTIVE, FINISHED
	}
	protected HashMap<SpliceOperation,List<SpliceOperation>> operationTree;
	protected SpliceOperation currentExecutionOperation;
	protected LinkedList<SpliceOperation> allWaits = new LinkedList<SpliceOperation>();
	protected NoPutResultSet output;
	public OperationTree() {
		this.operationTree = new LinkedHashMap<SpliceOperation,List<SpliceOperation>>();
	}
	@Override
	public void traverse(SpliceOperation operation) {
		SpliceLogUtils.trace(LOG, "traversing parent operation: " + operation);
		currentExecutionOperation = operation;
		traverseSingleTree(currentExecutionOperation);
		while ( (currentExecutionOperation = allWaits.poll()) != null) {
			SpliceLogUtils.trace(LOG,"   traversing wait operation: " + currentExecutionOperation);
			traverseSingleTree(currentExecutionOperation);
		}		
		if (LOG.isTraceEnabled()) {	
			LOG.trace("Operation Tree Generated:");
			for (SpliceOperation op: operationTree.keySet()) {
				LOG.trace("  Operation: " + op);
				for (SpliceOperation waitOp: operationTree.get(op)) {
					LOG.trace("      Waiting on Operation: " + waitOp);
				}
			}
		}
	}
	
	private void traverseSingleTree(SpliceOperation operation) {
		SpliceLogUtils.trace(LOG, "traverseSingleTree " + operation);
		for (SpliceOperation spliceOperation: operation.getSubOperations()) {
			if (spliceOperation.getNodeTypes().contains(NodeType.REDUCE)) {
				SpliceLogUtils.trace(LOG,"found reduce operation %s",spliceOperation);
				allWaits.push(spliceOperation);
				if (operationTree.containsKey(operation))
					operationTree.get(currentExecutionOperation).add(spliceOperation);
				else { 
					List<SpliceOperation> newWaits = new ArrayList<SpliceOperation>();
					newWaits.add(spliceOperation);
					operationTree.put(currentExecutionOperation, newWaits);
				}
			}  
			else if (spliceOperation.getNodeTypes().contains(NodeType.MAP) || spliceOperation.getNodeTypes().contains(NodeType.SCAN)) {
				traverseSingleTree(spliceOperation);				
			}
		}	
		if (!operationTree.containsKey(currentExecutionOperation)) {
			operationTree.put(currentExecutionOperation, new ArrayList<SpliceOperation>());
		}
	} 
	
	public void schedule() {
		
	}
	@Override
	public NoPutResultSet execute() throws StandardException{	
		SpliceLogUtils.trace(LOG, "execute with operationTree with  " + operationTree.keySet().size() + " execution stacks");
		SpliceOperation operation = null;
		while (operationTree.keySet().size() > 0) {
			for (SpliceOperation op: operationTree.keySet())
				if (operationTree.get(op).size() == 0) {
					SpliceLogUtils.trace(LOG, "Executing Set of Operations with executing step " + op);
					if (op.getNodeTypes().contains(NodeType.REDUCE)){
						op.executeShuffle();
						//if we are also a scan, AND we are the last operationTree to execute, then create the scan
						if(op.getNodeTypes().contains(NodeType.SCAN)&&operationTree.keySet().size()==1)
							output = op.executeScan();
					}else
						output = op.executeScan();
					operation = op;
					operationFinished(op.getUniqueSequenceID());
				}			
		}	
		SpliceLogUtils.trace(LOG,"Execution Tree Finalized: %s with object %s", operation,output); 
		return output;
	}
	
	private void operationFinished(String uniqueID) {
		SpliceLogUtils.trace(LOG, "operation " + uniqueID + " finished, cleaning up operation tree");
		for (SpliceOperation parentOperation : operationTree.keySet()) {
			if (parentOperation.getUniqueSequenceID().equals(uniqueID)) {
				operationTree.remove(parentOperation);
				break;
			}
		}
		for (List<SpliceOperation> waitOperations : operationTree.values()) {
			for (SpliceOperation wait : waitOperations) {
				if (wait.getUniqueSequenceID().equals(uniqueID)) {
					waitOperations.remove(wait);
					break;
				}
			}
		}
	}
}