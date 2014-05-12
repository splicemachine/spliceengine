package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.List;
import java.util.NavigableMap;
import java.util.concurrent.*;


/**
 * Traverses the operation stack to form Serialization boundaries in "levels". Each
 * level is then executed in parallel, but *all* operations from *all* lower levels
 * will complete *before* the next level is executed. E.g., if Operation K is at level i,
 * then all the levels 1,2,...i-1 MUST complete before K can be shuffled.
 *
 * @author Scott Fines
 *         Created on: 6/26/13
 */
public class OperationTree {
    private static final Logger LOG = Logger.getLogger(OperationTree.class);
    private static final ThreadPoolExecutor levelExecutor;

    static {
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("operation-shuffle-pool-%d")
                                        .setDaemon(true).build();

        levelExecutor = new ThreadPoolExecutor(SpliceConstants.maxTreeThreads,
                SpliceConstants.maxTreeThreads, 60, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),factory,
         new ThreadPoolExecutor.CallerRunsPolicy());

    }

    public static SpliceNoPutResultSet executeTree(SpliceOperation operation, final SpliceRuntimeContext runtimeContext,boolean useProbe) throws StandardException{
        //first form the level Map
        NavigableMap<Integer,List<SpliceOperation>> levelMap = split(operation);
        if (LOG.isDebugEnabled())
        	SpliceLogUtils.debug(LOG, "OperationTree levelMap: %s \n\tfor operation %s", levelMap, operation);

        //The levelMap is sorted so that lower level number means higher on the tree, so
        //since we need to execute from bottom up, we go in descending order
				final StatementInfo info = runtimeContext.getStatementInfo();
				boolean setStatement = info !=null;
				long statementUuid = setStatement? info.getStatementUuid():0l;
        for(Integer level:levelMap.descendingKeySet()){
            List<SpliceOperation> levelOps = levelMap.get(level);
            if(levelOps.size()>1){
                List<Future<Void>> shuffleFutures = Lists.newArrayListWithCapacity(levelOps.size());
                for(final SpliceOperation opToShuffle:levelOps){
										if(setStatement)
												opToShuffle.setStatementId(statementUuid);
                    shuffleFutures.add(levelExecutor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
                            boolean prepared = false;
                            try {
                                transactionResource.prepareContextManager();
                                prepared = true;
                                transactionResource.marshallTransaction(info.getTxnId());
                                long begin = System.currentTimeMillis();
                                opToShuffle.executeShuffle(runtimeContext);
                                System.out.println(String.format("Running operation %s taking %d seconds",opToShuffle,System.currentTimeMillis()-begin));
                            } finally {
                                resetContext(transactionResource, prepared);
                            }
                            return null;
                        }
                    }));
                }
                //wait for all operations to complete before proceeding to the next level
                for(Future<Void> future:shuffleFutures){
                    try {
                        future.get();
                    } catch (InterruptedException e) {
                        //TODO -sf- cancel other futures!
                        throw Exceptions.parseException(e);
                    } catch (ExecutionException e) {
                        //TODO -sf- cancel other futures!
                        throw Exceptions.parseException(e);
                    }
                }
            }else{
                for(SpliceOperation op:levelOps){
										if(setStatement)
												op.setStatementId(statementUuid);
                    op.executeShuffle(runtimeContext);
                }
            }
        }
        //operation is the highest level, it has the final scan
				if(useProbe)
						return operation.executeProbeScan();
				else
						return operation.executeScan(runtimeContext);
    }

    private static void resetContext(SpliceTransactionResourceImpl impl, boolean prepared) {
        if(prepared){
            impl.resetContextManager();
        }
        if (impl != null) {
            impl.cleanup();
        }
    }

    private static NavigableMap<Integer, List<SpliceOperation>> split(SpliceOperation parentOperation) {
        NavigableMap<Integer,List<SpliceOperation>> levelMap = Maps.newTreeMap();
        if(parentOperation.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE))
            levelMap.put(0, Arrays.asList(parentOperation));
        split(parentOperation, levelMap, 1);
        return levelMap;
    }

    private static void split(SpliceOperation parentOp,NavigableMap<Integer,List<SpliceOperation>> levelMap, int level){
				List<SpliceOperation> levelOps = levelMap.get(level);
				List<SpliceOperation> children;
				if(parentOp instanceof NestedLoopJoinOperation){
						/*
						 * NestedLoopJoin shouldn't execute a shuffle on it's right side,
						 * but it SHOULD if there's a shuffle on the left side
						 */
						children = Arrays.asList(parentOp.getLeftOperation());
				}else
						children = parentOp.getSubOperations();
        for(SpliceOperation child:children){
            if(child.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE)){
                if(levelOps==null){
                    levelOps = Lists.newArrayListWithCapacity(children.size());
                    levelMap.put(level,levelOps);
                }
                levelOps.add(child);
            }
            split(child,levelMap,level+1);
        }
    }

		public static int getNumSinks(SpliceOperation topOperation) {
				List<SpliceOperation> children = topOperation.getSubOperations();
				int numSinks = 0;
				for(SpliceOperation child:children){
						numSinks+=getNumSinks(child);
				}
				if(topOperation.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE))
						numSinks++;
				return numSinks;
		}

}
