package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Sink;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import jsr166y.ForkJoinPool;
import jsr166y.ForkJoinTask;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;

import java.sql.SQLException;
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
        return executeInFJ(operation, runtimeContext, useProbe);
        /*

        //first form the level Map
        NavigableMap<Integer, List<SpliceOperation>> levelMap = split(operation);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "OperationTree levelMap: %s \n\tfor operation %s", levelMap, operation);

        //The levelMap is sorted so that lower level number means higher on the tree, so
        //since we need to execute from bottom up, we go in descending order
        final StatementInfo info = runtimeContext.getStatementInfo();
        boolean setStatement = info != null;
        long statementUuid = setStatement ? info.getStatementUuid() : 0l;
        for (Integer level : levelMap.descendingKeySet()) {
            List<SpliceOperation> levelOps = levelMap.get(level);
            if (levelOps.size() > 1) {
                List<Future<Void>> shuffleFutures = Lists.newArrayListWithCapacity(levelOps.size());
                for (final SpliceOperation opToShuffle : levelOps) {
                    if (setStatement)
                        opToShuffle.setStatementId(statementUuid);
                    shuffleFutures.add(levelExecutor.submit(new Callable<Void>() {
                        @Override
                        public Void call() throws Exception {
                            SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
                            boolean prepared = false;
                            try {
                                transactionResource.prepareContextManager();
                                prepared = true;
                                transactionResource.marshallTransaction(
                                        opToShuffle.getActivation().getTransactionController().getActiveStateTxIdString());
                                long begin = System.currentTimeMillis();
                                opToShuffle.executeShuffle(runtimeContext);
                                LOG.debug(String.format("Running shuffle for operation %s taking %dms",
                                                           opToShuffle.resultSetNumber(),
                                                           System.currentTimeMillis() - begin));
                            } finally {
                                resetContext(transactionResource, prepared);
                            }
                            return null;
                        }
                    }));
                }
                //wait for all operations to complete before proceeding to the next level
                for (Future<Void> future : shuffleFutures) {
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
            } else {
                for (SpliceOperation op : levelOps) {
                    if (setStatement)
                        op.setStatementId(statementUuid);
                    long begin = System.currentTimeMillis();
                    op.executeShuffle(runtimeContext);
                    LOG.debug(String.format("Running shuffle for operation %s taking %dms",
                                               op.resultSetNumber(),
                                               System.currentTimeMillis() - begin));
                }
            }
        }

        //operation is the highest level, it has the final scan
        SpliceNoPutResultSet nprs;
        long begin = System.currentTimeMillis();
        if (useProbe) {
            nprs = operation.executeProbeScan();
        } else {
            nprs = operation.executeScan(runtimeContext);
        }
        LOG.debug(String.format("Returning scan result set for operation %s (took %dms)",
                                   operation.resultSetNumber(),
                                   System.currentTimeMillis() - begin));
        return nprs;
        */
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

    private final static ForkJoinPool fjpool = new ForkJoinPool(10);

    public static SpliceNoPutResultSet executeInFJ(SpliceOperation root, final SpliceRuntimeContext ctx, boolean probe)
            throws StandardException {

        List<SinkingOperation> deps = immediateSinkDependencies(root);
        if (deps.size() > 0){
            List<ForkJoinTask<Boolean>> futures = Lists.newArrayListWithExpectedSize(deps.size());
            for (SinkingOperation s: deps){
                futures.add(fjpool.submit(sinkTask(s, ctx)));
            }
            for (ForkJoinTask<Boolean> f: Lists.reverse(futures)) {
                f.join();
            }
        }
        if (isSink(root)){
            shuffle((SinkingOperation)root, ctx);
        }
        return probe ? root.executeProbeScan() : root.executeScan(ctx);
    }

    public static boolean isSink(SpliceOperation op) {
        return op.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE);
    }

    public static ForkJoinTask<Boolean> sinkTask(final SinkingOperation sink, final SpliceRuntimeContext ctx) {
        return new ForkJoinTask<Boolean>() {
            @Override
            public Boolean getRawResult() {
                return null;
            }

            @Override
            protected void setRawResult(Boolean o) {

            }

            @Override
            protected boolean exec() {
                try {
                    List<ForkJoinTask<Boolean>> depTasks = Lists.newLinkedList();
                    for (SinkingOperation s: immediateSinkDependencies(sink)){
                        LOG.error(String.format("Adding sink dep %s for %s", s.resultSetNumber(), sink.resultSetNumber()));
                        depTasks.add(sinkTask(s, ctx));
                    }
                    // run dependent sinks & wait for completion
                    invokeAll(depTasks);
                    LOG.error(String.format("%s done waiting for deps", sink.resultSetNumber()));
                    runSettingThreadLocals(sink, ctx);
                    return true;
                } catch (SQLException e) {
                    throw new RuntimeException(Exceptions.parseException(e));
                } catch (StandardException e) {
                    throw new RuntimeException(Exceptions.parseException(e));
                }
            }
        };
    }

    public static void runSettingThreadLocals(SinkingOperation op, SpliceRuntimeContext ctx)
            throws StandardException, SQLException {
        SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
        boolean prepared = false;
        try {
            long begin = System.currentTimeMillis();
            transactionResource.prepareContextManager();
            prepared = true;
            transactionResource.marshallTransaction(
                  op.getActivation().getTransactionController().getActiveStateTxIdString());
            shuffle(op, ctx);
            LOG.debug(String.format("Running shuffle for operation %s taking %dms",
                     op.resultSetNumber(),
                     System.currentTimeMillis() - begin));
        } finally {
            resetContext(transactionResource, prepared);
        }
    }

    public static void shuffle(SinkingOperation op, SpliceRuntimeContext ctx) throws StandardException {
        final StatementInfo info = ctx.getStatementInfo();
        boolean setStatement = info != null;
        long statementUuid = setStatement ? info.getStatementUuid() : 0;
        if (setStatement){
            op.setStatementId(statementUuid);
        }
        op.executeShuffle(ctx);
    }

    public static List<SinkingOperation> immediateSinkDependencies(SpliceOperation op){
        List<SinkingOperation> sinks = Lists.newLinkedList();
        List<SpliceOperation> children = op instanceof NestedLoopJoinOperation ?
                                             Arrays.asList(op.getLeftOperation()) :
                                             op.getSubOperations();
        for (SpliceOperation child: children) {
            if (isSink(child)) {
                sinks.add((SinkingOperation) child);
            } else if (child != null) {
                sinks.addAll(immediateSinkDependencies(child));
            }
        }
        return sinks;
    }

}
