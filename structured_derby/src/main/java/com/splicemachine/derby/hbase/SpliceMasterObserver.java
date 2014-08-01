package com.splicemachine.derby.hbase;

import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.coprocessor.BaseMasterObserver;
import org.apache.hadoop.hbase.coprocessor.MasterCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Responsible for actions (create system tables, restore tables) that should only happen on one node.
 */
public class SpliceMasterObserver extends BaseMasterObserver {

    private static final Logger LOG = Logger.getLogger(SpliceMasterObserver.class);

    public static final byte[] INIT_TABLE = Bytes.toBytes("SPLICE_INIT");
    public static final byte[] RESTORE_TABLE = Bytes.toBytes("SPLICE_RESTORE");

    private SpliceMasterObserverRestoreAction restoreAction;
    private SpliceMasterObserverInitAction initAction;

    @Override
    public void start(CoprocessorEnvironment ctx) throws IOException {
        SpliceLogUtils.info(LOG, "Starting SpliceMasterObserver");
        restoreAction = new SpliceMasterObserverRestoreAction(((MasterCoprocessorEnvironment) ctx).getMasterServices());
        initAction = new SpliceMasterObserverInitAction();
    }

    @Override
    public void stop(CoprocessorEnvironment ctx) throws IOException {
        SpliceLogUtils.info(LOG, "Stopping SpliceMasterObserver");
    }

    @Override
    public void preCreateTable(ObserverContext<MasterCoprocessorEnvironment> ctx, HTableDescriptor desc, HRegionInfo[] regions) throws IOException {
        SpliceLogUtils.info(LOG, "preCreateTable %s", Bytes.toString(desc.getName()));

        if (Bytes.equals(desc.getName(), INIT_TABLE)) {
            initAction.execute();
        }
        if (Bytes.equals(desc.getName(), RESTORE_TABLE)) {
            restoreAction.restoreDatabase(desc);
        }
    }


}