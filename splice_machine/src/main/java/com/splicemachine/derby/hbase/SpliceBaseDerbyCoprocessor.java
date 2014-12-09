package com.splicemachine.derby.hbase;

import java.util.concurrent.atomic.AtomicLong;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import com.splicemachine.si.impl.TransactionalRegions;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Coprocessor for starting the derby services on top of HBase.
 *
 * @author John Leach
 */
public class SpliceBaseDerbyCoprocessor {
    private static final AtomicLong runningCoprocessors = new AtomicLong(0l);
    private boolean tableEnvMatch;
    public static volatile String regionServerZNode;
    public static volatile String rsZnode;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */

    public void start(CoprocessorEnvironment e) {
        rsZnode = ((RegionCoprocessorEnvironment) e).getRegionServerServices().getZooKeeper().rsZNode;
        regionServerZNode =((RegionCoprocessorEnvironment) e).getRegionServerServices().getServerName().getServerName();

        SpliceConstants.TableEnv tableEvn = EnvUtils.getTableEnv((RegionCoprocessorEnvironment)e);
        tableEnvMatch = !SpliceConstants.TableEnv.ROOT_TABLE.equals(tableEvn);

        //make sure the factory is correct
        TransactionalRegions.setActionFactory(RollForwardAction.FACTORY);
        //use the independent write control from the write pipeline
        TransactionalRegions.setTrafficControl(SpliceBaseIndexEndpoint.independentTrafficControl);
        if (tableEnvMatch) {
        	SpliceDriver.driver().start(((RegionCoprocessorEnvironment) e).getRegionServerServices());
            runningCoprocessors.incrementAndGet();
        }
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    public void stop(CoprocessorEnvironment e) {
            if (runningCoprocessors.decrementAndGet() <= 0l) {
                SpliceDriver.driver().shutdown();
            }
    }

}

