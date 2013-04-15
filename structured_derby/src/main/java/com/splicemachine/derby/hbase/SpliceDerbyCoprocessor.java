package com.splicemachine.derby.hbase;

import java.util.concurrent.atomic.AtomicLong;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si2.utils.SIUtils;
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
        tableEnvMatch = SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.USER_TABLE)
                || SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.USER_INDEX_TABLE)
                || SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.DERBY_SYS_TABLE)
                || SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TxnConstants.TableEnv.META_TABLE);

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

