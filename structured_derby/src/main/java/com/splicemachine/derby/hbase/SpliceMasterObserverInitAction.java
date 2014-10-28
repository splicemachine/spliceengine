package com.splicemachine.derby.hbase;

import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.exception.SpliceDoNotRetryIOException;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Init tables.
 */
class SpliceMasterObserverInitAction {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
    private static final AtomicReference<State> state = new AtomicReference<State>();

    private volatile Future<Void> createFuture;
    private ExecutorService executor;

    SpliceMasterObserverInitAction() {
        executor = MoreExecutors.namedSingleThreadExecutor("splice-master-manager");
        state.set(State.NOT_STARTED);
    }


    /**
     * Unusual method contract here, always throws:
     *
     * @throws PleaseHoldException   initialization is in progress an region servers should wait
     * @throws DoNotRetryIOException database init has completed
     * @throws IOException           database init failed
     */
    public void execute() throws IOException {
        try {
            SpliceLogUtils.info(LOG, "Creating splice init table:" + this.toString());
            evaluateState();
            createSplice();
            throw new PleaseHoldException("pre create succeeded");
        } catch (PleaseHoldException phe) {
            // Expected when state != RUNNING, catch at this level to avoid logging full stack trace.
            throw phe;
        } catch (DoNotRetryIOException dnr) {
            throw dnr;
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "preCreateTable Error", Exceptions.getIOException(e));
        }
    }

    protected void evaluateState() throws Exception {
        //check for startup errors
        if (createFuture != null && createFuture.isDone()) {
            createFuture.get(); //this will throw an error if the startup sequence fails
        }
        boolean success = false;
        do {
            State currentState = state.get();
            switch (currentState) {
                case INITIALIZING:
                    throw new PleaseHoldException("Please Hold - Starting");
                case RUNNING:
                    throw new SpliceDoNotRetryIOException("Success");
                case NOT_STARTED:
                    success = state.compareAndSet(currentState, State.INITIALIZING);
            }
        } while (!success);
    }

    private void createSplice() throws Exception {
        createFuture = executor.submit(new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                Connection connection = null;
                try {
                    if (ZkUtils.isSpliceLoaded()) {
                        SpliceLogUtils.info(LOG, "Splice Machine has already been initialized");
                        state.set(State.RUNNING);
                        return null;
                    } else {
                        SpliceLogUtils.info(LOG, "Initializing Splice Machine for first time");
                        ZkUtils.refreshZookeeper();
                        SpliceUtilities.refreshHbase();
                        SpliceUtilities.createSpliceHBaseTables();
                        new SpliceAccessManager(); //make sure splice access manager gets loaded
                        //make sure that we have a Snowflake loaded
                        SpliceDriver.driver().loadUUIDGenerator();
                        EmbedConnectionMaker maker = new EmbedConnectionMaker();
                        connection = maker.createFirstNew();
                        ZkUtils.spliceFinishedLoading();
                        state.set(State.RUNNING);
                        return null;
                    }
                } catch (Exception e) {
                    SpliceLogUtils.logAndThrow(LOG, e);
                } finally {
                    if (connection != null)
                        connection.close();
                }
                return null;

            }

        });
    }

    @Override
    public String toString() {
        return "state=" + state.get() + ", createFuture.isDone=" + (createFuture == null ? "NULL" : createFuture.isDone());
    }

    private static enum State {
        NOT_STARTED,
        INITIALIZING,
        RUNNING
    }


}
