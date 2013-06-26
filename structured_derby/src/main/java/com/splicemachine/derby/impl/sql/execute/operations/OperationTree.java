package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.*;

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
		SpliceLogUtils.trace(LOG, "traversing parent operation: %s",operation);
		currentExecutionOperation = operation;
		traverseSingleTree(currentExecutionOperation);
		while ( (currentExecutionOperation = allWaits.poll()) != null) {
			SpliceLogUtils.trace(LOG,"   traversing wait operation: %s",currentExecutionOperation);
			traverseSingleTree(currentExecutionOperation);
		}		
		if (LOG.isTraceEnabled()) {	
			SpliceLogUtils.trace(LOG,"Operation Tree Generated:");
			for (SpliceOperation op: operationTree.keySet()) {
				SpliceLogUtils.trace(LOG,"  Operation: %s",op);
				for (SpliceOperation waitOp: operationTree.get(op)) {
					SpliceLogUtils.trace(LOG,"      Waiting on Operation: %s",waitOp);
				}
			}
		}
	}
	
	private void traverseSingleTree(SpliceOperation operation) {
		SpliceLogUtils.trace(LOG, "traverseSingleTree %s", operation);
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
		SpliceLogUtils.trace(LOG, "execute with operationTree with %d execution stacks",operationTree.keySet().size());
		SpliceOperation operation = null;
		while (operationTree.keySet().size() > 0) {
            Iterator<Map.Entry<SpliceOperation, List<SpliceOperation>>> treeIterator = operationTree.entrySet().iterator();
            while(treeIterator.hasNext()){
                Map.Entry<SpliceOperation,List<SpliceOperation>> ops = treeIterator.next();
                SpliceOperation op = ops.getKey();
                if(op.getNodeTypes().contains(NodeType.REDUCE)){
                    op.executeShuffle();
                    if(op.getNodeTypes().contains(NodeType.SCAN)&&!treeIterator.hasNext())
                        output = op.executeScan();
                }else
                    output = op.executeScan();

                operation = op;
                List<SpliceOperation> waitOperations = ops.getValue();

                //remove the operation from operationTree
                treeIterator.remove();

                Iterator<SpliceOperation> waitOps = waitOperations.iterator();
                String uniqueSequenceId = op.getUniqueSequenceID();
                while(waitOps.hasNext()){
                    SpliceOperation wait = waitOps.next();
                    if(wait.getUniqueSequenceID().equals(uniqueSequenceId)){
                        waitOperations.remove(wait);
                        break;
                    }
                }

            }
//			for (SpliceOperation op: operationTree.keySet())
//				if (operationTree.get(op).size() == 0) {
//					SpliceLogUtils.trace(LOG, "Executing Set of Operations with executing step %s",op);
//					if (op.getNodeTypes().contains(NodeType.REDUCE)){
//						op.executeShuffle();
//						//if we are also a scan, AND we are the last operationTree to execute, then create the scan
//						if(op.getNodeTypes().contains(NodeType.SCAN)&&operationTree.keySet().size()==1)
//							output = op.executeScan();
//					}else
//						output = op.executeScan();
//					operation = op;
//					operationFinished(op.getUniqueSequenceID());
//				}
		}
		SpliceLogUtils.trace(LOG,"Execution Tree Finalized: %s with object %s", operation,output); 
		return output;
	}
	
	private void operationFinished(String uniqueID) {
		SpliceLogUtils.trace(LOG, "operation %s finished, cleaning up operation tree",uniqueID);
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