package com.splicemachine.derby.hbase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseRowProcessorEndpoint;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.environment.EnvUtils;

/**
 * Coprocessor for starting the derby services on top of HBase.
 *
 * @author John Leach
 */
public class SpliceDerbyCoprocessor extends BaseRowProcessorEndpoint {
    private static final AtomicLong runningCoprocessors = new AtomicLong(0l);
    private boolean tableEnvMatch;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    @Override
    public void start(CoprocessorEnvironment e) throws IOException {
        tableEnvMatch = EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(SpliceConstants.TableEnv.USER_TABLE)
                || EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(SpliceConstants.TableEnv.USER_INDEX_TABLE)
                || EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(SpliceConstants.TableEnv.DERBY_SYS_TABLE)
                || EnvUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(SpliceConstants.TableEnv.META_TABLE);

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
        if (tableEnvMatch) {
            if (runningCoprocessors.decrementAndGet() <= 0l) {
                SpliceDriver.driver().shutdown();
            }
        }
    }

}

