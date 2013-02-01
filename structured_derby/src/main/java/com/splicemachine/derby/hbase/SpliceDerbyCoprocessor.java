package com.splicemachine.derby.hbase;

import java.io.IOException;

import org.apache.derby.drda.NetworkServerControl;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.log4j.Logger;

import com.splicemachine.derby.logging.DerbyOutputLoggerWriter;
import com.splicemachine.utils.SpliceLogUtils;
/**
 * Derby Days?
 * 
 * @author johnleach
 *
 */
public class SpliceDerbyCoprocessor extends BaseEndpointCoprocessor {
	private static Logger LOG = Logger.getLogger(SpliceDerbyCoprocessor.class);
	protected static NetworkServerControl server;
	public static String SPLICE_OBSERVER_INSTRUCTIONS = "SpliceServerInstructions";
	/**
	 * Logs the start of the observer.
	 */
	@Override
	public void start(CoprocessorEnvironment e) {
		SpliceLogUtils.info(LOG, "Starting the coprocessor CoProcessor %s", SpliceDerbyCoprocessor.class);
		super.start(e);
		synchronized (this) {
			if (server == null) {
				try {
					server = new NetworkServerControl();
					server.start(new DerbyOutputLoggerWriter()); // This will log to log4j
					server.logConnections(true);
					SpliceLogUtils.info(LOG, server.getSysinfo());
				} catch (Exception exception) {
					SpliceLogUtils.logAndThrowRuntime(LOG, "Could Not Start Derby - Catastrophic", exception);
				}
			}
		}
	}
	/**
	 * Logs the stop of the observer.
	 */
	@Override
	public void stop(CoprocessorEnvironment e) {
		SpliceLogUtils.info(LOG, "Stopping the CoProcessor %s",SpliceDerbyCoprocessor.class);
		super.stop(e);
		synchronized (this) {
			if (server != null) {
				try {
					server = null;
				} catch (Exception exception) {
					SpliceLogUtils.logAndThrowRuntime(LOG, "Could Not Start Derby - Catastrophic", exception);
				}
			}
		}

	}
	
}

