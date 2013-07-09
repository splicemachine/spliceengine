package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.derby.error.SpliceDoNotRetryIOException;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.PleaseHoldException;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;

public class SpliceMasterObserver extends BaseMasterObserver {
	public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");
    private ExecutorService executor;
    protected static State state;
    public static enum State{
        NOT_STARTED,
        INITIALIZING,
        RUNNING
    }
	private static Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
	@Override
	public void start(CoprocessorEnvironment ctx) throws IOException {
		SpliceLogUtils.debug(LOG, "Starting Splice Master Observer");
        ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("splice-master-manager").build();
        executor = Executors.newSingleThreadExecutor(factory);
        state = State.NOT_STARTED;
		super.start(ctx);
	}

	@Override
	public void stop(CoprocessorEnvironment ctx) throws IOException {
		SpliceLogUtils.debug(LOG, "Stopping Splice Master Observer");
		super.stop(ctx);
	}
	
	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "preCreateTable %s",Bytes.toString(desc.getName()));
		if (!Bytes.equals(desc.getName(),INIT_TABLE))
			super.preCreateTable(ctx, desc, regions);			
		else {
			try {
		        SpliceLogUtils.info(LOG, "Creating Splice");
		        evaluateState();
				createSplice();
				throw new PleaseHoldException("pre create succeeeded");
			} catch (PleaseHoldException phe) {
				throw phe;
			} catch (DoNotRetryIOException dnr) {
				throw dnr;
			}
			catch (Exception e) {
				SpliceLogUtils.logAndThrow(LOG, "preCreateTable Error", Exceptions.getIOException(e));
			}
		}
	}
	
	protected synchronized static void evaluateState() throws Exception {
		switch (state) {
		case INITIALIZING:
			throw new PleaseHoldException("Please Hold - Starting");
		case NOT_STARTED:
			state = State.INITIALIZING;
			return;
		case RUNNING:
			throw new SpliceDoNotRetryIOException("Success");
		default:
			break;
		}
	}

	private void createSplice() throws Exception {
		executor.submit(new Callable<Void>() {
			@Override
			public Void call() throws Exception {
				Connection connection = null;
				try {
					if (ZkUtils.isSpliceLoaded()) {
						SpliceLogUtils.info(LOG, "Splice Already Loaded");
						state = State.RUNNING;
						return null;
					} else {
						SpliceLogUtils.info(LOG, "Booting Splice");
						ZkUtils.refreshZookeeper();
						SpliceUtilities.refreshHbase();
						SpliceUtilities.createSpliceHBaseTables();

		                new SpliceAccessManager(); //make sure splice access manager gets loaded
		                //make sure that we have a Snowflake loaded
		                SpliceDriver.driver().loadUUIDGenerator();
						EmbedConnectionMaker maker = new EmbedConnectionMaker();
						connection = maker.createNew();
						ZkUtils.spliceFinishedLoading();
						state = State.RUNNING;
						return null;
					}
				} catch (Exception e) {
					SpliceLogUtils.logAndThrow(LOG, e);
				}
				finally {
					if(connection!=null)
		                connection.close();
				}
				return null;

			}
			
		});
	}
	
}
