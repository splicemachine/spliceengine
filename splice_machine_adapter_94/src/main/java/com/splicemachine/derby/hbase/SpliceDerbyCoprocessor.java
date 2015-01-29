package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionServerCoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.RegionServerObserver;

import java.io.IOException;

/**
 * Coprocessor for starting the derby services on top of HBase.
 *
 * @author John Leach
 */
public class SpliceDerbyCoprocessor extends BaseEndpointCoprocessor implements RegionServerObserver {
    private SpliceBaseDerbyCoprocessor impl;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        impl = new SpliceBaseDerbyCoprocessor();
        impl.start(e);
        super.start(e);
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
        impl.stop(e);
    }

    @Override
    public void preStopRegionServer(ObserverContext<RegionServerCoprocessorEnvironment> regionServerCoprocessorEnvironmentObserverContext) throws IOException {
        impl.stoppingRegionServer();
    }
}

