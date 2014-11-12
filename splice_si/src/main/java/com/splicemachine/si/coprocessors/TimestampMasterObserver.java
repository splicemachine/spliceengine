package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.timestamp.TimestampServer;
import com.splicemachine.si.impl.timestamp.TimestampUtil;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Master observer coprocessor responsible for starting
 * the specialized server {@link TimestampServer} that provides
 * timestamps for the transaction system.
 */

public class TimestampMasterObserver extends BaseMasterObserver {

	private static Logger LOG = Logger.getLogger(TimestampMasterObserver.class);

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        TimestampUtil.doServerInfo(LOG, "Starting Timestamp Master Observer...");
        
        ZooKeeperWatcher zkw = ((MasterCoprocessorEnvironment)ctx).getMasterServices().getZooKeeper();
        RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
        
        new TimestampServer(SpliceConstants.timestampServerBindPort, rzk).startServer();
        
        super.start(ctx);
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
    	TimestampUtil.doServerInfo(LOG, "Stopping Timestamp Master Observer...");
        super.stop(ctx);
    }
}
