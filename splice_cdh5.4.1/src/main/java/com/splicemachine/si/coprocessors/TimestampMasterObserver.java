package com.splicemachine.si.coprocessors;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.timestamp.api.TimestampBlockManager;
import com.splicemachine.timestamp.hbase.ZkTimestampBlockManager;
import com.splicemachine.timestamp.impl.TimestampServer;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
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

    private TimestampServer timestampServer;

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        LOG.info("Starting Timestamp Master Observer");
        
        ZooKeeperWatcher zkw = ((MasterCoprocessorEnvironment)ctx).getMasterServices().getZooKeeper();
        RecoverableZooKeeper rzk = zkw.getRecoverableZooKeeper();
        TimestampBlockManager tbm= new ZkTimestampBlockManager(rzk,SIConstants.zkSpliceMaxReservedTimestampPath);
        this.timestampServer =new TimestampServer(SIConstants.timestampServerBindPort,tbm,SIConstants.timestampBlockSize);

        this.timestampServer.startServer();
        
        super.start(ctx);
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        LOG.warn("Stopping Timestamp Master Observer");
        this.timestampServer.stopServer();
    }
}
