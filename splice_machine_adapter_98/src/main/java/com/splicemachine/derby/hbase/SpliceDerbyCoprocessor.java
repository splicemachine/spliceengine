package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import com.google.protobuf.Service;
import com.splicemachine.coprocessor.SpliceMessage.SpliceDerbyCoprocessorService;

public class SpliceDerbyCoprocessor extends SpliceDerbyCoprocessorService implements CoprocessorService, Coprocessor {
	public SpliceBaseDerbyCoprocessor impl;

    /**
     * Logs the start of the observer and runs the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    @Override
    public void start(CoprocessorEnvironment e) {
    	impl = new SpliceBaseDerbyCoprocessor();
    	impl.start((RegionCoprocessorEnvironment) e);
    }

    /**
     * Logs the stop of the observer and shutdowns the SpliceDriver if needed...
     * 
     * @see com.splicemachine.derby.hbase.SpliceDriver
     * 
     */
    @Override
    public void stop(CoprocessorEnvironment e) {
    	impl.stop((RegionCoprocessorEnvironment) e);
    }

	@Override
	public Service getService() {
		return this;
	}

}