package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.job.Task;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Pair;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public interface RegionTask extends Task {

    void prepareTask(byte[] start,byte[] end,RegionCoprocessorEnvironment rce,
                     SpliceZooKeeperManager zooKeeper ) throws ExecutionException;

    /**
     * @return true if task should be invalidated when a region closes.
     */
    boolean invalidateOnClose();

    String getTaskNode();

		RegionTask getClone();

		boolean isSplittable();
}
