
package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation.NodeType;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.KeyDecoder;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.job.JobResults;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.NoPutResultSet;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;

public class OperationUtils {

	private OperationUtils(){}

		public static Callable<Void> cleanupSubTasks(final SpliceOperation op){
				return new Callable<Void>(){

						@Override
						public Void call() throws Exception {
								List<SpliceOperation> subOps = op.getSubOperations();
								for(SpliceOperation subOp:subOps){
										cleanupSubTask(subOp);
								}
								return null;
						}
				};
		}

		private static void cleanupSubTask(SpliceOperation op) throws StandardException {
				JobResults jobResults = op.getJobResults();
				if(jobResults!=null)
						jobResults.cleanup();

				List<SpliceOperation> subOperations = op.getSubOperations();
				for(SpliceOperation subOp:subOperations){
						cleanupSubTask(subOp);
				}
		}

		public static void generateLeftOperationStack(SpliceOperation op,List<SpliceOperation> opAccumulator){
		SpliceOperation leftOp = op.getLeftOperation();
		if(leftOp !=null && !leftOp.getNodeTypes().contains(NodeType.REDUCE)){
			//recursively generateLeftOperationStack
			generateLeftOperationStack(leftOp,opAccumulator);
		}else if(leftOp!=null)
			opAccumulator.add(leftOp);
		opAccumulator.add(op);
	}
	
	public static List<SpliceOperation> getOperationStack(SpliceOperation op){
		List<SpliceOperation> ops = new LinkedList<SpliceOperation>();
		generateLeftOperationStack(op,ops);
		return ops;
	}

		public static NoPutResultSet executeScan(SpliceOperation operation,Logger log) throws StandardException {
				SpliceLogUtils.trace(log,"executeScan");
				final List<SpliceOperation> operationStack = new ArrayList<SpliceOperation>();
				operation.generateLeftOperationStack(operationStack);
				SpliceLogUtils.trace(log, "operationStack=%s",operationStack);
				SpliceOperation regionOperation = operationStack.get(0);
				SpliceLogUtils.trace(log,"regionOperation=%s",regionOperation);
				RowProvider provider;
				SpliceRuntimeContext spliceRuntimeContext = new SpliceRuntimeContext();
				PairDecoder decoder = OperationUtils.getPairDecoder(operation,spliceRuntimeContext);
				try {
						if (regionOperation.getNodeTypes().contains(NodeType.REDUCE) && operation != regionOperation) {
								SpliceLogUtils.trace(log,"scanning Temp Table");
								provider = regionOperation.getReduceRowProvider(operation,decoder,spliceRuntimeContext, true);
						} else {
								SpliceLogUtils.trace(log,"scanning Map Table");
								provider = regionOperation.getMapRowProvider(operation,decoder,spliceRuntimeContext);
						}
				} catch (IOException e) {
						throw Exceptions.parseException(e);
				}
				return new SpliceNoPutResultSet(operation.getActivation(),operation, provider);
	}

		public static PairDecoder getPairDecoder(SpliceOperation operation,SpliceRuntimeContext runtimeContext) throws StandardException {
				KeyDecoder keyDecoder = operation.getKeyEncoder(runtimeContext).getDecoder();
				KeyHashDecoder rowDecoder = operation.getRowHash(runtimeContext).getDecoder();
				return new PairDecoder(keyDecoder,rowDecoder,operation.getExecRowDefinition());
		}

		/**
		 * Determines if an operation is operation solely over in-memory representations. (e.g. There is no
		 * active serialization over to a region for scanning.)
		 *
		 * @param operation the operation to check
		 * @return true if this operation is acted upon entirely in memory
		 */
		public static boolean isInMemory(SpliceOperation operation){
				if(operation==null) return false;
				if(operation instanceof RowOperation) return true;
				if(operation instanceof SinkingOperation) return false; //sinking ops always go to TEMP, can't be in-memory

				List<SpliceOperation> subOperations = operation.getSubOperations();
				/*
				 * If we got here, we can't be a RowOperation, but we're still a leaf node. Thus, we aren't an
				 * in-memory node, so return false
				 */
				if(subOperations.size()<=0) return false;
				for(SpliceOperation child: subOperations){
						if(!isInMemory(child)) return false;
				}
				return true;
		}
}
