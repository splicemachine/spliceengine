package com.splicemachine.derby.hbase;

import com.splicemachine.concurrent.MoreExecutors;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.exception.SpliceDoNotRetryIOException;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Init tables.
 */
class SpliceMasterObserverInitAction {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
    private static final AtomicReference<State> state = new AtomicReference<>();
    private volatile Future<Void> createFuture;
    private ExecutorService executor;

    SpliceMasterObserverInitAction() {
        executor = MoreExecutors.namedSingleThreadExecutor("splice-master-manager", true);
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
        } catch (PleaseHoldException | DoNotRetryIOException phe) {
            // Expected when state != RUNNING, catch at this level to avoid logging full stack trace.
            throw phe;
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
                        SpliceUtilities.createRestoreTableIfNecessary();
                        // Boot up the Splice data dictionary to trigger the upgrade process if it is needed.
                        // We don't want the region servers to attempt an upgrade since we could end up in a race condition
                        // between the region servers when the system tables are being updated.  See DB-2011 for more details.
                        connection = bootSplice();
                        state.set(State.RUNNING);
                        return null;
                    } else {
                        SpliceLogUtils.info(LOG, "Initializing Splice Machine for first time");
                        ZkUtils.refreshZookeeper();
                        SpliceUtilities.createSpliceHBaseTables();
                        connection = bootSplice();
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

    /**
     * Boot up Splice (Derby) by creating an internal embedded connection to it.
	 * @return internal connection to Splice
	 * @throws IOException
	 * @throws SQLException
	 */
	private Connection bootSplice() throws IOException, SQLException {
		new SpliceAccessManager(); //make sure splice access manager gets loaded
		//make sure that we have a Snowflake loaded
		SpliceDriver.driver().loadUUIDGenerator(SpliceConstants.config.getInt(HConstants.MASTER_PORT, 60010));
		EmbedConnectionMaker maker = new EmbedConnectionMaker();
		return maker.createFirstNew();
	}

    @Override
    public String toString() {
        return "state=" + state.get() + ", createFuture.isDone=" + (createFuture == null ? "NULL" : createFuture.isDone());
    }

    public void onMasterStop() {
        state.set(State.SHUTTING_DOWN);

        /* Why are we calling SpliceDriver.shutdown() on master?  Apparently the creation of an embedded connection
         * in this class as part of startup ends up starting, here on master: task framework thread pool, region cache
         * loading threads, asyc-hbase new-io thread pools, and lots of other stuff.  None of this is necessary on
         * master so this is bad and should be fixed.  In the mean time, to get master to actually shutdown gracefully
         * we have to stop that stuff and SpliceDriver.shutdown() is a convenient way to do it.  */
        SpliceDriver.driver().shutdown();

        executor.shutdownNow();
    }

    private enum State {
        NOT_STARTED,
        SHUTTING_DOWN,
        INITIALIZING,
        RUNNING
    }

}
