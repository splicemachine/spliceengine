package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.tools.EmbedConnectionMaker;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceUtilities;
import com.splicemachine.utils.ZkUtils;


public class SpliceMasterObserver extends BaseMasterObserver {
	public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");
    public static enum State{
        NOT_STARTED,
        INITIALIZING,
        RUNNING,
        STARTUP_FAILED, SHUTDOWN
    }
	private static Logger LOG = Logger.getLogger(SpliceMasterObserver.class);
	@Override
	public void start(CoprocessorEnvironment ctx) throws IOException {
		SpliceLogUtils.debug(LOG, "Starting Splice Master Observer");
		super.start(ctx);
	}

	@Override
	public void stop(CoprocessorEnvironment ctx) throws IOException {
		SpliceLogUtils.debug(LOG, "Stopping Splice Master Observer");
		super.stop(ctx);
	}
	
	@Override
	public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.debug(LOG, "preCreateTable %s",Bytes.toString(desc.getName()));
		if (!Bytes.equals(desc.getName(),INIT_TABLE))
			super.preCreateTable(ctx, desc, regions);			
		else {
			try {
		        SpliceLogUtils.debug(LOG, "Creating Splice");
				createSplice();
			} catch (Exception e) {
				throw SpliceStandardLogUtils.generateSpliceIOException(LOG, "preCreateTable Error", e);
			} finally {
				throw new DoNotRetryIOException("pre create succeeeded");
			}
			
		}
	}

	private synchronized void createSplice() throws Exception {
		Connection connection = null;
		try {
			if (ZkUtils.isSpliceLoaded()) {
				SpliceLogUtils.debug(LOG, "Splice Already Loaded");
				return;
			} else {
				SpliceLogUtils.debug(LOG, "Booting Splice");
				ZkUtils.refreshZookeeper();
				SpliceUtilities.refreshHbase();
				SpliceUtilities.createSpliceHBaseTables();
				EmbedConnectionMaker maker = new EmbedConnectionMaker();
				connection = maker.createNew();
				ZkUtils.spliceFinishedLoading();
			}
		} catch (Exception e) {
			SpliceLogUtils.logAndThrow(LOG, e);
		}
		finally {
			if(connection!=null)
                connection.close();
		}
	}
	
}
