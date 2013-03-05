package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.util.concurrent.atomic.AtomicLong;
/**
 * Derby Days?
 * 
 * @author johnleach
 *
 */
public class SpliceDerbyCoprocessor extends BaseEndpointCoprocessor {
    private static final AtomicLong runningCoprocessors = new AtomicLong(0l);
	/**
	 * Logs the start of the observer.
	 */
	@Override
	public void start(CoprocessorEnvironment e) {
        SpliceDriver.driver().start();
        runningCoprocessors.incrementAndGet();
		super.start(e);
	}

	/**
	 * Logs the stop of the observer.
	 */
	@Override
	public void stop(CoprocessorEnvironment e) {
        if(runningCoprocessors.decrementAndGet()<=0l){
            SpliceDriver.driver().shutdown();
        }
	}
	
}

