package com.splicemachine.compactions;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.configuration.HBaseConfiguration;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.hbase.ZkUtils;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;

import java.util.Arrays;

/**
 * Created by dgomezferro on 6/17/16.
 *
 * This task books N slots from a given Spark pool so not all tasks are used by other pools
 */
public class PoolSlotBooker implements Runnable {
    private static final Logger LOG = Logger.getLogger(PoolSlotBooker.class);
    String group;
    String pool;
    int slots;
    String previous;
    JavaFutureAction<?> previousAction;
    int counter;

    public PoolSlotBooker(String group, String pool, int slots) {
        this.group = group;
        this.pool = pool;
        this.slots = slots;
    }

    @Override
    public void run() {
        try {
            DistributedDataSetProcessor dsp = EngineDriver.driver().processorFactory().distributedProcessor();
            dsp.setJobGroup(group + "-" + counter++, "Ignore, reserved slot for " + pool);
            dsp.setSchedulerPool(pool);

            Object[] array = new Object[slots];
            String parent = HConfiguration.getConfiguration().getSpliceRootPath() + HBaseConfiguration.BOOKINGS_PATH + "/";
            String path = ZkUtils.getRecoverableZooKeeper().create(parent, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
            JavaFutureAction<?> action = SpliceSpark.getContext().parallelize(Arrays.asList(array), slots).map(new PlaceholderTask(path, 10)).map(new PlaceholderTask(path, 10)).collectAsync();
            if (previous != null) {
                // cancel previous job
                previousAction.cancel(false);
                ZkUtils.delete(previous);
            }
            this.previous = path;
            this.previousAction = action;
        } catch (Throwable t) {
            LOG.error("Caught exception, retry on next iteration", t);
        }
    }
}
