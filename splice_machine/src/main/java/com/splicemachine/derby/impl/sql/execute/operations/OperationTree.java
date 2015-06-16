package com.splicemachine.derby.impl.sql.execute.operations;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceNoPutResultSet;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.spark.RDDRowProvider;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.management.StatementInfo;
import com.splicemachine.pipeline.exception.Exceptions;

import com.splicemachine.db.iapi.error.StandardException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;


/**
 * Traverses the operation tree executing the "shuffle", "parallel", or "sinking" (the
 * latter is the term used in code here) phases of nodes that have them, from the
 * leaves up. For a given sinking node, all of its descendant sinking nodes must be sunk
 * before it can itself sink. After all sinking nodes in the tree have been sunk, the
 * "serial" or "scan" phase of the root node can be run, and execution of the tree is
 * complete.
 *
 * We use the ForkJoin framework just to make better use of threads. A naive implementation
 * of the traverse-and-wait approach used here would occupy as many threads as non-root
 * sinking nodes, as in this recursive pseudo-code:
 *
 *  sink(node):
 *   for each sinking child of node:
 *      run sink(child) on a separate thread
 *   join those threads
 *   run node's sinking phase
 *
 * But using ForkJoin, the join call in the above pseudo-code does not actually block
 * a thread. The ForkJoin worker thread will run other sink tasks while waiting for
 * child sinks to complete.
 *
 */
public class OperationTree {
    private static final Logger LOG = Logger.getLogger(OperationTree.class);
    private final static ForkJoinPool FJ_POOL = new ForkJoinPool(SpliceConstants.maxTreeThreads);

    public static void sink(SpliceOperation root, SpliceRuntimeContext ctx) throws StandardException, IOException {
        if (root instanceof ExplainOperation)
            return;
        List<SinkingOperation> deps = immediateSinkDependencies(root);
        if (deps.size() > 0) {
            List<ForkJoinTask> futures = Lists.newArrayListWithExpectedSize(deps.size());
            for (SinkingOperation s : deps) {
                futures.add(FJ_POOL.submit(sinkAction(s, ctx)));
            }
            for (ForkJoinTask f : Lists.reverse(futures)) {
                f.join();
            }
        }
        if (isSink(root)) {
            shuffle((SinkingOperation) root, ctx);
        }
    }

    /**
     * Returns the sinking node children of op (where a sinking node child is a descendant
     * sinking reachable only through non-sinking nodes).
     */
    public static List<SinkingOperation> immediateSinkDependencies(SpliceOperation op) {
        List<SinkingOperation> sinks = Lists.newLinkedList();
        List<SpliceOperation> children = op instanceof NestedLoopJoinOperation ?
                Arrays.asList(op.getLeftOperation()) :
                op.getSubOperations();
        for (SpliceOperation child : children) {
            if (isSink(child)) {
                sinks.add((SinkingOperation) child);
            } else if (child != null) {
                sinks.addAll(immediateSinkDependencies(child));
            }
        }
        return sinks;
    }

    public static boolean isSink(SpliceOperation op) {
        return op.getNodeTypes().contains(SpliceOperation.NodeType.REDUCE);
    }

    public static int getNumSinks(SpliceOperation op) {
        int n = 0;
        for (SinkingOperation child : immediateSinkDependencies(op)) {
            n = n + getNumSinks(child);
        }
        return n + (isSink(op) ? 1 : 0);
    }

    public static RecursiveAction sinkAction(final SinkingOperation sink,
                                             final SpliceRuntimeContext ctx) {
        return new RecursiveAction() {
            @Override
            protected void compute() {
                try {
                    List<RecursiveAction> depTasks = Lists.newLinkedList();
                    for (SinkingOperation s : immediateSinkDependencies(sink)) {
                        depTasks.add(sinkAction(s, ctx));
                    }
                    // run dependent sinks & wait for completion
                    invokeAll(depTasks);
                    runSettingThreadLocals(sink, ctx);
                } catch (SQLException | StandardException | IOException e) {
                    throw new RuntimeException(Exceptions.parseException(e));
                }
            }
        };
    }

    private static void resetContext(SpliceTransactionResourceImpl impl, boolean prepared) {
        if (prepared) {
            impl.resetContextManager();
        }
        if (impl != null) {
            impl.cleanup();
        }
    }

    public static void runSettingThreadLocals(SinkingOperation op, SpliceRuntimeContext ctx)
            throws StandardException, SQLException, IOException {
        SpliceTransactionResourceImpl transactionResource = new SpliceTransactionResourceImpl();
        boolean prepared = false;
        try {
            transactionResource.prepareContextManager();
            prepared = true;
            shuffle(op, ctx);
        } finally {
            resetContext(transactionResource, prepared);
        }
    }

    public static void shuffle(SinkingOperation op, SpliceRuntimeContext ctx) throws StandardException, IOException {
        long begin = System.currentTimeMillis();
        final StatementInfo info = ctx.getStatementInfo();
        boolean setStatement = info != null;
        long statementUuid = setStatement ? info.getStatementUuid() : 0;
        if (setStatement) {
            op.setStatementId(statementUuid);
        }
        op.executeShuffle(ctx);
        if (LOG.isDebugEnabled()) {
            LOG.debug(String.format("Running shuffle for operation %s taking %dms",
                    op.resultSetNumber(),
                    System.currentTimeMillis() - begin));
        }
    }

}
