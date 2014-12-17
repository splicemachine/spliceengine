package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.Txn;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.impl.BaseSIFilter;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * RowProvider which uses Key-matching to ensure safe execution
 * in the face of multiple RowProviders being used in Parallel.
 *
 * It handles cases were part of the rows needed for an aggregation /
 * merge sort are on different sides of a split. One alternative would
 * be to make sure splits always happen on the right places (aggregation
 * boundaries...) but this has problems of its own:
 *  - regions could get very big if the aggregation cardinality is low
 *  (group by <field with few values>)
 *  - region splitter would need to know where splits can happen
 *  - it could create uneven splits 
 *
 * @author Scott Fines
 *         Created: 1/17/13 2:49 PM
 */
public class RegionAwareScanner extends ReopenableScanner implements SpliceResultScanner {
    private static final Logger LOG = Logger.getLogger(RegionAwareScanner.class);
    private final ScanBoundary boundary;
    private final HRegion region;
    private final HTableInterface table;
    private MeasuredRegionScanner localScanner;
    private ResultScanner lookBehindScanner;
    private ResultScanner lookAheadScanner;
    private boolean lookBehindExhausted = false;
    private boolean localExhausted = false;
    private boolean lookAheadExhausted = false;
    private List keyValues = Lists.newArrayList();
    private byte[] remoteFinish;
    private byte[] remoteStart;
    private byte[] localStart;
    private byte[] localFinish;
    private byte[] regionFinish;
    private byte[] regionStart;
    private final Txn txn;
    private final Scan scan;
    private Scan localScan;
    private long localRows;

		//statistics stuff
		private final MetricFactory metricFactory;
		private final Timer remoteReadTimer;
		private final Counter remoteBytesRead;
		private Scan lookAheadScan;
		private Scan lookBehindScan;

		private RegionAwareScanner(Txn txn,
															 HTableInterface table,
															 HRegion region,
															 Scan scan,
															 ScanBoundary scanBoundary, MetricFactory metricFactory){
        this.table = table;
        this.region = region;
        this.boundary = scanBoundary;
        this.scan =scan;
				this.metricFactory = metricFactory;
				if(region!=null){
            this.regionFinish = region.getEndKey();
            this.regionStart = region.getStartKey();
        }else{
            this.regionFinish = scan.getStartRow();
            this.regionStart = scan.getStopRow();
        }
				this.txn = txn;
				this.remoteReadTimer = metricFactory.newWallTimer();
				this.remoteBytesRead = metricFactory.newCounter();
    }

    private RegionAwareScanner(Txn txn,
															 HTableInterface table,
															 HRegion region,
															 byte[] scanStart,
															 byte[] scanFinish,
															 ScanBoundary scanBoundary, MetricFactory metricFactory){
				this(txn,table,region,new Scan(scanStart,scanFinish),scanBoundary,metricFactory);
    }

		@Override public TimeView getRemoteReadTime() { return remoteReadTimer.getTime(); }
		@Override public long getRemoteBytesRead() { return remoteBytesRead.getTotal(); }
		@Override public long getRemoteRowsRead() { return remoteReadTimer.getNumEvents(); }

		@Override public TimeView getLocalReadTime() { return localScanner.getReadTime(); }
		@Override public long getLocalBytesRead() { return localScanner.getBytesOutput(); }
		@Override public long getLocalRowsRead() { return localScanner.getRowsOutput(); }

		/**
     * @return the new RowResult in the scan, or {@code null} if no more rows are to be returned.
     */
    public Result getNextResult() throws IOException {
        Result currentResult = null;
        //get next row from scanner
        if(!lookBehindExhausted){
            remoteReadTimer.startTiming();
            try {
                currentResult = lookBehindScanner.next();
            } catch (IOException e) {
                if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                    SpliceLogUtils.trace(LOG, "Re-create lookBehindScanner scanner with startRow = %s", BytesUtil.toHex(getLastRow()));
                    incrementNumRetries();
                    lookBehindScanner = reopenResultScanner(lookBehindScanner, lookBehindScan, table);
                    currentResult = getNextResult();
                }
                else {
                    SpliceLogUtils.logAndThrowRuntime(LOG, e);
                }
            }
            if(currentResult!=null&&!currentResult.isEmpty()) {
                remoteReadTimer.tick(1);
                if(remoteBytesRead.isActive()){
                    measureResult(currentResult);
                }
                setLastRow(currentResult.getRow());
                return currentResult;
            }else{
                remoteReadTimer.tick(0);
                lookBehindExhausted=true;
								setLastRow(null); //make sure that you reopen from the start of the scan if needed
            }
        }

        if(!localExhausted){
            if(keyValues==null)
                keyValues = Lists.newArrayList();
            keyValues.clear();
            try {
                localExhausted = !localScanner.next(keyValues);
            } catch (IOException e) {
                if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                    SpliceLogUtils.trace(LOG, "Re-create localScanner scanner with startRow = %s", BytesUtil.toHex(getLastRow()));
                    incrementNumRetries();
                    localScanner = reopenRegionScanner(localScanner, region, localScan, metricFactory);
                    currentResult = getNextResult();
                }
                else {
                    SpliceLogUtils.logAndThrowRuntime(LOG, e);
                }
            }
            if(keyValues.size()>0){
                setLastRow(dataLib.getDataRow(keyValues.get(keyValues.size()-1)));
                localRows++;
                return new Result(keyValues);
            }else{
                localExhausted=true;
								setLastRow(null); //make sure that you reopen from the start of the lookAhead scan if needed
						}
        }

        if(!lookAheadExhausted){
            remoteReadTimer.startTiming();
            try {
                currentResult = lookAheadScanner.next();
            } catch (IOException e) {
                if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
										if(LOG.isTraceEnabled())
												SpliceLogUtils.trace(LOG, "Re-create lookAheadScanner scanner with startRow = %s", BytesUtil.toHex(getLastRow()));
                    incrementNumRetries();
                    lookAheadScanner = reopenResultScanner(lookAheadScanner, lookAheadScan, table);
                    currentResult = getNextResult();
                }
                else {
                    SpliceLogUtils.logAndThrowRuntime(LOG, e);
                }
            }

            if(currentResult!=null&&!currentResult.isEmpty()){
                remoteReadTimer.tick(1);
                if(remoteBytesRead.isActive()){
                    measureResult(currentResult);
                }
                setLastRow(currentResult.getRow());
                return currentResult;
            }else{
                remoteReadTimer.tick(0);
                lookAheadExhausted = true;
            }
        }

        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "read %d rows from remote", remoteReadTimer.getNumEvents());
            SpliceLogUtils.debug(LOG, "read %d rows from local", localRows);
            byte[] scanStart = scan.getStartRow();
            byte[] scanFinish = scan.getStopRow();
            if (scanStart != null)
                SpliceLogUtils.debug(LOG,"scanStart         = %s", BytesUtil.toHex(scanStart));
            if (scanFinish != null)
                SpliceLogUtils.debug(LOG,"scanStart         = %s", BytesUtil.toHex(scanFinish));
            if (regionStart != null)
                SpliceLogUtils.debug(LOG,"regionStart       = %s", BytesUtil.toHex(regionStart));
            if (regionFinish != null)
                SpliceLogUtils.debug(LOG,"regionFinish      = %s", BytesUtil.toHex(regionFinish));
            if (remoteStart!=null)
                SpliceLogUtils.debug(LOG,"remoteStart       = %s", BytesUtil.toHex(remoteStart));
            if (remoteFinish != null)
                SpliceLogUtils.debug(LOG,"remoteFinish      = %s", BytesUtil.toHex(remoteFinish));
            if (localStart != null)
                SpliceLogUtils.debug(LOG,"localStart        = %s", BytesUtil.toHex(localStart));
            if (localFinish != null)
                SpliceLogUtils.debug(LOG,"localFinish       = %s", BytesUtil.toHex(localFinish));
            if (localScanner != null)
                SpliceLogUtils.debug(LOG, "localScanner     = %s", localScanner);
            if (lookAheadScanner != null)
                SpliceLogUtils.debug(LOG, "lookAheadScanner = %s", lookAheadScanner);
            if (lookBehindScanner != null)
                SpliceLogUtils.debug(LOG, "lookBehindScanner= %s", lookBehindScanner);
        }
        return null;
    }

		protected void measureResult(Result currentResult) {
				long byteSize = 0;
				for (KeyValue aRaw : currentResult.raw()) {
						byteSize += aRaw.getLength();
				}
				remoteBytesRead.add(byteSize);
		}

		/**
     * Create a new RegionAwareScanner from the region and tableName.
     *
     * @param region the region to scan over
     * @param boundary the boundary strategy to use
     * @param tableName the name of the table to scan over
     * @param scanStart the global start of the scan
     * @param scanFinish the global end to the scan
     * @return a RegionAwareScanner which can complete the portions of the global scan which
     * {@code region} is responsible for.
     */
    public static RegionAwareScanner create(Txn txn, HRegion region, ScanBoundary boundary,
                                            byte[] tableName,
                                            byte[] scanStart,
                                            byte[] scanFinish,
																						MetricFactory metricFactory){
        HTableInterface table = SpliceAccessManager.getHTable(tableName);
        return new RegionAwareScanner(txn,table,region,scanStart,scanFinish,boundary, metricFactory);
    }

    public static RegionAwareScanner create(Txn txn, HRegion region, Scan localScan,
                                            byte[] tableName,ScanBoundary boundary, MetricFactory metricFactory){
        HTableInterface table = SpliceAccessManager.getHTable(tableName);
        return new RegionAwareScanner(txn,table,region,localScan,boundary, metricFactory);
    }

    public void open() throws StandardException {
        try{
            buildScans();
        }catch(Exception ioe){
        	SpliceLogUtils.error(LOG, ioe);
        	throw Exceptions.parseException(ioe);
        }
    }

    @Override
    public Result next() throws IOException {
        return getNextResult();
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        List<Result> results = Lists.newArrayListWithExpectedSize(nbRows);
        for(int i=0;i<nbRows;i++){
            Result r = next();
            if(r==null) break;
            results.add(r);
        }

        return results.toArray(new Result[results.size()]);
    }

    @Override
    public void close() {
//				if(LOG.isDebugEnabled()){
//						LOG.debug(String.format("Saw %d rows from lookBehind scanner", lookBehindRowsSeen));
//						LOG.debug(String.format("Saw %d rows from local scanner",localRowsSeen));
//						LOG.debug(String.format("Saw %d rows from lookAhead scanner",lookAheadRowsSeen));
//				}
        if(lookBehindScanner !=null) lookBehindScanner.close();
        if(lookAheadScanner!=null) lookAheadScanner.close();
        try{
            if(localScanner!=null) localScanner.close();
            if(table!=null) table.close();
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
    }

    private void buildScans() throws IOException {
        byte[] scanStart = scan.getStartRow();
        byte[] scanFinish = scan.getStopRow();
        if(Arrays.equals(scanStart,scanFinish)){
            //empty scan, so it's not going to do anything anyway
            localStart = scanStart;
            localFinish=scanFinish;
            lookBehindExhausted=true;
            lookAheadExhausted=true;
        }else{
            //deal with the end of the region
        	handleEndOfRegion();
            //deal with the start of the region
        	handleStartOfRegion();
        }
        localScan = boundary.buildScan(txn,localStart,localFinish);
        localScan.setFilter(scan.getFilter());
				localScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
				if(SpliceConstants.useReadAheadScanner)
						localScanner = new ReadAheadRegionScanner(region,
										SpliceConstants.DEFAULT_CACHE_SIZE,
										region.getScanner(localScan), metricFactory,HTransactorFactory.getTransactor().getDataLib() );
				else
						localScanner = new BufferedRegionScanner(region,
										region.getScanner(localScan), localScan,
										SpliceConstants.DEFAULT_CACHE_SIZE, metricFactory,HTransactorFactory.getTransactor().getDataLib()  );

				localScanner.start();
				if(remoteStart!=null){
            lookBehindScan = boundary.buildScan(txn,remoteStart,regionFinish);
            lookBehindScan.setFilter(scan.getFilter());
            lookBehindScanner = table.getScanner(lookBehindScan);
        }if(remoteFinish!=null){
            lookAheadScan = boundary.buildScan(txn,regionFinish,remoteFinish);
            lookAheadScan.setFilter(scan.getFilter());
            lookAheadScanner = table.getScanner(lookAheadScan);
        }
    }
    
    private void handleEndOfRegion() throws IOException {
        byte[] scanFinish = scan.getStopRow();
    	//deal with the end of the region
        if(Bytes.compareTo(regionFinish,scanFinish)>=0 || regionFinish.length<=0){
            //cool, no remote ends!
            if (regionFinish != null)
                SpliceLogUtils.debug(LOG, "handleEndOfRegion(): regionFinish = %s", BytesUtil.toHex(regionFinish));

            if (scanFinish != null)
                SpliceLogUtils.debug(LOG, "handleEndOfRegion(): scanFinish   = %s", BytesUtil.toHex(scanFinish));

            localFinish = scanFinish;
            lookAheadExhausted = true;
        }else{
            //have to determine whether to lookahead or stop early.
            Scan aheadScan = boundary.buildScan(txn,regionFinish,scanFinish);
            aheadScan.setCaching(1);
            aheadScan.setBatch(1);
            //carry over filters from localscan
            aheadScan.setFilter(getCorrectFilter(scan.getFilter(),txn));
            ResultScanner aheadScanner = null;
            try{
                aheadScanner = table.getScanner(aheadScan);
                Result firstNotInRegion = aheadScanner.next();
                if (firstNotInRegion == null|| firstNotInRegion.isEmpty()) { // No values, exhaust
                    localFinish = regionFinish;
                    lookAheadExhausted=true;
                }else{
	                byte[] finalKeyStart = boundary.getStartKey(firstNotInRegion);
	                if (finalKeyStart == null) {
	                    localFinish = regionFinish;
	                    lookAheadExhausted=true;
	                    return;
	                }
	                
	                if(Bytes.compareTo(finalKeyStart,regionFinish)>=0){
	                    //that key is contained in the other region, so we are good to
	                    //just scan to the end of the region without lookaheads or
	                    //terminating early
	                    localFinish = regionFinish;
	                    lookAheadExhausted=true;
	                }else if(boundary.shouldLookAhead(finalKeyStart)){
	                    remoteFinish = boundary.getStopKey(firstNotInRegion);
	                    localFinish = regionFinish;
	                }else if(boundary.shouldStopEarly(finalKeyStart)){
	                    localFinish = finalKeyStart;
	                    lookAheadExhausted=true;
	                }
                }
            }finally{
                if(aheadScanner!=null)aheadScanner.close();
            }
        }

    }
    private void handleStartOfRegion() throws IOException {
        byte[] scanStart = scan.getStartRow();
        byte[] scanStop = scan.getStopRow();
        //deal with the start of the region
        if(Bytes.compareTo(scanStart,regionStart)>0||regionStart.length<=0||BytesUtil.intersect(scanStart, scanStop, regionStart, regionFinish) == null){
            //cool, no remoteStarts!
            if (LOG.isDebugEnabled()) {
                if (regionStart != null)
                    SpliceLogUtils.debug(LOG, "handleStartOfRegion(): regionStart  = %s", BytesUtil.toHex(regionStart));
                if (regionFinish != null)
                    SpliceLogUtils.debug(LOG, "handleStartOfRegion(): regionFinish = %s", BytesUtil.toHex(regionFinish));
                if (scanStart != null)
                    SpliceLogUtils.debug(LOG, "handleStartOfRegion(): scanStart    = %s", BytesUtil.toHex(scanStart));
                if (scanStop != null)
                    SpliceLogUtils.debug(LOG, "handleStartOfRegion(): scanStop     = %s", BytesUtil.toHex(scanStop));
            }
            localStart = scanStart;
            lookBehindExhausted=true;
        }else{
            //have to determine whether or not to lookbehind or skip the first local elements
            Scan startScan = boundary.buildScan(txn,regionStart,regionFinish);
            startScan.setCaching(1);
            startScan.setBatch(1);
            //carry over scan filters
            startScan.setFilter(getCorrectFilter(scan.getFilter(), txn));
            RegionScanner localScanner = null;
            ResultScanner behindScanner = null;
            try{
            	localScanner = new BufferedRegionScanner(region,region.getScanner(startScan),
                        startScan,startScan.getCaching(),metricFactory,HTransactorFactory.getTransactor().getDataLib());
                List keyValues = Lists.newArrayList();
                localScanner.next(keyValues);
                if (keyValues.isEmpty()) {
                	// need to do something here...
                    localStart = regionStart;
                    lookBehindExhausted=true;
                    return;
                }
                Result firstRowInRegion = dataLib.newResult(keyValues);
                byte[] startKey = boundary.getStartKey(firstRowInRegion);
                if (startKey == null) {
                    localStart = regionStart;
                    lookBehindExhausted=true;
                    return;
                }
                	
                if(Bytes.compareTo(startKey,regionStart)>0){
                    //the key starts entirely in this region, so we don't need
                    //to worry about lookbehinds or skipping ahead
                    localStart = regionStart;
                    lookBehindExhausted=true;
                }else {
                    // if startKey <= regionStart, need to look back
                    Scan behindScan = boundary.buildScan(txn, startKey, regionStart);
                    behindScan.setCaching(1);
                    behindScan.setBatch(1);

                    behindScanner = table.getScanner(behindScan);
                    Result r = behindScanner.next();
                    if (r == null) {
                        //If no rows are in [startKey, regionStart), then this region begins with a new startKey
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "Rows with startKey %s starts from the beginning of region %s",
                                    BytesUtil.toHex(startKey), region.getRegionNameAsString());
                        }
                        localStart = regionStart;
                        lookBehindExhausted=true;
                    }
                    else {
                        // there are rows in the range [startKey, regionStart). These rows must be in a previous region.
                        // skip rows with this startKey in the current region
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "Skip rows with startKey %s at the beginning of region %s",
                                    BytesUtil.toHex(startKey), region.getRegionNameAsString());
                        }
                        localStart = boundary.getStopKey(firstRowInRegion);
                        lookBehindExhausted = true;
                    }
                }
            }finally{
                if(localScanner!=null)
                    localScanner.close();

                if (behindScanner != null)
                    behindScanner.close();
            }
        }
    }

    private Filter getCorrectFilter(Filter filter, Txn txn) {
				if(txn!=null||filter==null)
						return filter;
        /*
         * If we have no transaction id, we need to make sure and remove the SI Filter from the list,
         * because otherwise it'll break
         */
        if(filter instanceof BaseSIFilter) return null;
        else if(filter instanceof FilterList){
            FilterList list = (FilterList)filter;
            FilterList copy = new FilterList();
            boolean added = false;
            for(Filter listedFilter:list.getFilters()){
                if(!(listedFilter instanceof BaseSIFilter)){
                    added=true;
                    copy.addFilter(listedFilter);
                }
            }
            if(added)
                return copy;
            else
                return null;
        }else return filter;
    }

		public byte[] getTableName() {
        return region.getTableDesc().getName();
    }

    @Override
    public Iterator<Result> iterator() {
        return null;
    }

}
