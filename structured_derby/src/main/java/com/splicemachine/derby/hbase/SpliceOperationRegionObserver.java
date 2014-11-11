package com.splicemachine.derby.hbase;

import com.splicemachine.async.HbaseAttributeHolder;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.pipeline.api.Service;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.utils.SpliceLogUtils;

import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Region Observer looking for a scan with <i>SpliceServerInstructions</i> set on the attribute map of the scan.
 *
 * @author johnleach
 *
 */
public class SpliceOperationRegionObserver extends BaseRegionObserver {
    private static Logger LOG = Logger.getLogger(SpliceOperationRegionObserver.class);
    public static String SPLICE_OBSERVER_INSTRUCTIONS = "Z"; // Reducing this so the amount of network traffic will be reduced...
    public static DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;
    private TransactionalRegion txnRegion;

    /**
     * Logs the start of the observer.
     */
    @Override
    public void start(final CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.info(LOG, "Starting TransactionalManagerRegionObserver CoProcessor %s", SpliceOperationRegionObserver.class);

        SpliceDriver.driver().registerService(new Service() {
            @Override
            public boolean start() {
                //TODO -sf- implement RollForward
                HRegion region = ((RegionCoprocessorEnvironment) e).getRegion();
                SpliceOperationRegionObserver.this.txnRegion = TransactionalRegions.get(region);
                return true;
            }

            @Override
            public boolean shutdown() {
                return false;
            }
        });
        SpliceLogUtils.info(LOG, "Started CoProcessor %s", SpliceOperationRegionObserver.class);
        super.start(e);
    }

    /**
     * Logs the stop of the observer.
     */
    @Override
    public void stop(CoprocessorEnvironment e) throws IOException {
        SpliceLogUtils.info(LOG, "Stopping the CoProcessor %s",SpliceOperationRegionObserver.class);
        super.stop(e);
        if(txnRegion!=null)
            txnRegion.discard();
    }

    @Override
    public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan, RegionScanner s) throws IOException {
        if(scan.getAttribute(SPLICE_OBSERVER_INSTRUCTIONS)!=null)
            return super.preScannerOpen(e,scan,s);

        Filter filter = scan.getFilter();
        if(filter instanceof HbaseAttributeHolder){
            setAttributesFromFilter(scan, (HbaseAttributeHolder) filter);
            scan.setFilter(null); //clear the filter
//            scan.setMaxVersions();
        }else if (filter instanceof FilterList){
            FilterList fl = (FilterList)filter;
            List<Filter> filters = fl.getFilters();
            Iterator<Filter> fIter = filters.iterator();
            while(fIter.hasNext()){
                Filter next = fIter.next();
                if(next instanceof HbaseAttributeHolder){
                    setAttributesFromFilter(scan,(HbaseAttributeHolder)next);
                    fIter.remove();
//                    scan.setMaxVersions();
                }
            }
        }
        return super.preScannerOpen(e, scan, s);
    }

    protected void setAttributesFromFilter(Scan scan, HbaseAttributeHolder filter) {
        Map<String,byte[]> attributes = ((HbaseAttributeHolder)filter).getAttributes();
        for(Map.Entry<String,byte[]> attribute:attributes.entrySet()){
            if(scan.getAttribute(attribute.getKey())==null)
                scan.setAttribute(attribute.getKey(),attribute.getValue());
        }
    }

    /**
	 * Override the postScannerOpen to wrap the scan with the SpliceOperationRegionScanner.  This allows for cases
	 * where the hbase scanner will be limited (ProjectRestrictOperation) or where new records would be added (LeftOuterJoin).
	 */
	@Override
	public RegionScanner postScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,RegionScanner s) throws IOException {
		if (scan.getAttribute(SPLICE_OBSERVER_INSTRUCTIONS) != null){
			SpliceLogUtils.trace(LOG, "postScannerOpen called, wrapping SpliceOperationRegionScanner");
			if (scan.getCaching() < 0) // Async Scanner is corrupting this value..
				scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
			return super.postScannerOpen(e, scan, derbyFactory.getOperationRegionScanner(s,scan,e.getEnvironment().getRegion(),txnRegion));
		}
		return super.postScannerOpen(e, scan, s);
	}

    @Override
    public void postScannerClose(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s) throws IOException {
        super.postScannerClose(e, s);
    }

    @Override
    public boolean preScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        if(s instanceof SpliceBaseOperationRegionScanner){
            ((SpliceBaseOperationRegionScanner)s).setupBatch();
        }
        return super.preScannerNext(e, s, results, limit, hasMore);
    }

    @Override
    public boolean postScannerNext(ObserverContext<RegionCoprocessorEnvironment> e, InternalScanner s, List<Result> results, int limit, boolean hasMore) throws IOException {
        if(s instanceof SpliceBaseOperationRegionScanner){
            ((SpliceBaseOperationRegionScanner)s).cleanupBatch();
        }
        return super.postScannerNext(e, s, results, limit, hasMore);
    }
}

