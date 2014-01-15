package com.splicemachine.derby.hbase;

import java.io.IOException;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;
/**
 * Region Observer looking for a scan with <i>SpliceServerInstructions</i> set on the attribute map of the scan.
 * 
 * @author johnleach
 *
 */
public class SpliceOperationRegionObserver extends BaseRegionObserver {
	private static Logger LOG = Logger.getLogger(SpliceOperationRegionObserver.class);
	public static String SPLICE_OBSERVER_INSTRUCTIONS = "Z"; // Reducing this so the amount of network traffic will be reduced...
	/**
	 * Logs the start of the observer.
	 */
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.info(LOG, "Starting TransactionalManagerRegionObserver CoProcessor %s", SpliceOperationRegionObserver.class);
		super.start(e);
	}
	/**
	 * Logs the stop of the observer.
	 */
	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.info(LOG, "Stopping the CoProcessor %s",SpliceOperationRegionObserver.class);
		super.stop(e);
	}

    /**
	 * Override the postScannerOpen to wrap the scan with the SpliceOperationRegionScanner.  This allows for cases
	 * where the hbase scanner will be limited (ProjectRestrictOperation) or where new records would be added (LeftOuterJoin).
	 */
	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,RegionScanner s) throws IOException {
		if (scan.getAttribute(SPLICE_OBSERVER_INSTRUCTIONS) != null){
			SpliceLogUtils.trace(LOG, "postScannerOpen called, wrapping SpliceOperationRegionScanner");
			return super.postScannerOpen(e, scan, new SpliceOperationRegionScanner(s,scan,e.getEnvironment().getRegion()));
		}
//		SpliceLogUtils.trace(LOG, "postScannerOpen called, but no instructions specified");
		return super.postScannerOpen(e, scan, s);
	}

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
        if(s instanceof SpliceOperationRegionScanner){
            ((SpliceOperationRegionScanner)s).reportMetrics();
        }
        super.postScannerClose(e, s);
    }
}

