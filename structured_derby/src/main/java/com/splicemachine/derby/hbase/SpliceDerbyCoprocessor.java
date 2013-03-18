package com.splicemachine.derby.hbase;

import java.util.concurrent.atomic.AtomicLong;

import com.splicemachine.constants.TxnConstants;
import com.splicemachine.si.utils.SIUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.log4j.Logger;

/**
 * Derby Days?
 *
 * @author johnleach
 */
public class SpliceDerbyCoprocessor extends BaseEndpointCoprocessor {
    private static final AtomicLong runningCoprocessors = new AtomicLong(0l);
    private boolean tableEnvMatch;

    /**
     * Logs the start of the observer.
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
     * Logs the stop of the observer.
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

