package com.splicemachine.derby.hbase;

import java.util.concurrent.atomic.AtomicLong;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

/**
 * Coprocessor for starting the derby services on top of HBase.
 *
 * @author John Leach
 */
public class SpliceDerbyCoprocessor extends BaseEndpointCoprocessor {
    private static final AtomicLong runningCoprocessors = new AtomicLong(0l);
    private boolean tableEnvMatch;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        SpliceConstants.TableEnv tableEvn = EnvUtils.getTableEnv((RegionCoprocessorEnvironment)e);
        tableEnvMatch = !SpliceConstants.TableEnv.ROOT_TABLE.equals(tableEvn);

        if (tableEnvMatch) {
            SpliceDriver.driver().start();
            runningCoprocessors.incrementAndGet();
        }
        super.start(e);
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
//        if (tableEnvMatch) {
            if (runningCoprocessors.decrementAndGet() <= 0l) {
                SpliceDriver.driver().shutdown();
            }
//        }
    }

}

