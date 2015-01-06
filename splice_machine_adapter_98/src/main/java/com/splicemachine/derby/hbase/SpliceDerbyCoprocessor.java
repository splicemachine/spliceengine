package com.splicemachine.derby.hbase;

import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;

public class SpliceDerbyCoprocessor extends SpliceDerbyCoprocessorService implements CoprocessorService, Coprocessor {

    private SpliceBaseDerbyCoprocessor impl;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     */
    @Override
    public void start(CoprocessorEnvironment e) {
        impl = new SpliceBaseDerbyCoprocessor();
        impl.start(e);
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
        impl.stop(e);
    }

    @Override
    public Service getService() {
        return this;
    }

}