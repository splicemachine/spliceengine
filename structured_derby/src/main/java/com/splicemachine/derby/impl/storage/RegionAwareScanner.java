package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.coprocessors.SIFilter;
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

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * RowProvider which uses Key-matching to ensure safe execution
 * in the face of multiple RowProviders being used in Parallel.
 *
 * @author Scott Fines
 *         Created: 1/17/13 2:49 PM
 */
public class RegionAwareScanner implements Closeable {
    private static final Logger LOG = Logger.getLogger(RegionAwareScanner.class);
    private final ScanBoundary boundary;
    private final HRegion region;
    private final HTableInterface table;
    private RegionScanner localScanner;
    private ResultScanner lookBehindScanner;
    private ResultScanner lookAheadScanner;
    private boolean lookBehindExhausted = false;
    private boolean localExhausted = false;
    private boolean lookAheadExhausted = false;
    private List<KeyValue> keyValues = Lists.newArrayList();
    private byte[] remoteFinish;
    private byte[] remoteStart;
    private byte[] localStart;
    private byte[] localFinish;
    private byte[] regionFinish;
    private byte[] regionStart;
    private final String transactionId;
    private final Scan scan;
    private byte[] tableName;

    private RegionAwareScanner(String transactionId,
                               HTableInterface table,
                               HRegion region,
                               Scan scan,
                               ScanBoundary scanBoundary){
        this.table = table;
        this.region = region;
        this.boundary = scanBoundary;
        this.scan =scan;
        if(region!=null){
            this.regionFinish = region.getEndKey();
            this.regionStart = region.getStartKey();
        }else{
            this.regionFinish = scan.getStartRow();
            this.regionStart = scan.getStopRow();
        }
        this.transactionId = transactionId;
    }

    private RegionAwareScanner(String transactionId,
                               HTableInterface table,
                               HRegion region,
                               byte[] scanStart,
                               byte[] scanFinish,
                               ScanBoundary scanBoundary){
        this.table = table;
        this.region = region;
        this.scan = new Scan(scanStart,scanFinish);
        this.boundary = scanBoundary;
        if(region!=null){
	        this.regionFinish = region.getEndKey();
	        this.regionStart = region.getStartKey();
        }else{
            this.regionFinish = scanFinish;
            this.regionStart = scanStart;
        }
        this.transactionId = transactionId;
    }

    /**
     * @return the new RowResult in the scan, or {@code null} if no more rows are to be returned.
     */
    public Result getNextResult(){
        Result currentResult;
        try{
	        //get next row from scanner
	        if(!lookBehindExhausted){
	            currentResult = lookBehindScanner.next();
	            if(currentResult!=null&&!currentResult.isEmpty()) return currentResult;
	            else
	                lookBehindExhausted=true;
	        }
	        if(!localExhausted){
                keyValues = new ArrayList<KeyValue>();
	            localExhausted = !localScanner.next(keyValues);
	            return new Result(keyValues);
	        }
	        if(!lookAheadExhausted){
	            currentResult = lookAheadScanner.next();
	            if(currentResult!=null&&!currentResult.isEmpty()) return currentResult;
	            else
	                lookAheadExhausted = true;
	        }
        }catch(IOException ioe){
            SpliceLogUtils.logAndThrowRuntime(LOG,ioe);
        }
        return null;
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
    public static RegionAwareScanner create(String transactionId, HRegion region, ScanBoundary boundary,
                                            byte[] tableName,
                                            byte[] scanStart,byte[] scanFinish){
        HTableInterface table = SpliceAccessManager.getHTable(tableName);
        return new RegionAwareScanner(transactionId,table,region,scanStart,scanFinish,boundary);
    }

    public static RegionAwareScanner create(String txnId, HRegion region, Scan localScan,
                                            byte[] tableName,ScanBoundary boundary){
        HTableInterface table = SpliceAccessManager.getHTable(tableName);
        return new RegionAwareScanner(txnId,table,region,localScan,boundary);
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
    public void close() throws IOException{
        if(lookBehindScanner !=null) lookBehindScanner.close();
        if(lookAheadScanner!=null) lookAheadScanner.close();
        if(localScanner!=null) localScanner.close();
        if(table!=null) table.close();
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
        Scan localScan = boundary.buildScan(transactionId,localStart,localFinish);
        localScan.setFilter(scan.getFilter());
        localScanner = region.getScanner(localScan);
        if(remoteStart!=null){
            Scan lookBehindScan = boundary.buildScan(transactionId,remoteStart,regionFinish);
            lookBehindScan.setFilter(scan.getFilter());
            lookBehindScanner = table.getScanner(lookBehindScan);
        }if(remoteFinish!=null){
            Scan lookAheadScan = boundary.buildScan(transactionId,remoteStart,regionFinish);
            lookAheadScan.setFilter(scan.getFilter());
            lookAheadScanner = table.getScanner(lookAheadScan);
        }
    }
    
    private void handleEndOfRegion() throws IOException {
        byte[] scanFinish = scan.getStopRow();
    	//deal with the end of the region
        if(Bytes.compareTo(regionFinish,scanFinish)>=0 || regionFinish.length<=0){
            //cool, no remote ends!
            localFinish = scanFinish;
            lookAheadExhausted = true;
        }else{
            //have to determine whether to lookahead or stop early.
            Scan aheadScan = boundary.buildScan(transactionId,regionFinish,scanFinish);
            aheadScan.setCaching(1);
            aheadScan.setBatch(1);
            //carry over filters from localscan
            aheadScan.setFilter(getCorrectFilter(scan.getFilter(),transactionId));
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
        //deal with the start of the region
        if(Bytes.compareTo(scanStart,regionStart)>=0||regionStart.length<=0){
            //cool, no remoteStarts!
            localStart = scanStart;
            lookBehindExhausted=true;
        }else{
            //have to determine whether or not to lookbehind or skip the first local elements
            Scan startScan = boundary.buildScan(transactionId,regionStart,regionFinish);
            startScan.setCaching(1);
            startScan.setBatch(1);
            //carry over scan filters
            startScan.setFilter(getCorrectFilter(scan.getFilter(), transactionId));
            RegionScanner localScanner = null;
            try{
                localScanner = region.getScanner(startScan);
                List<KeyValue> keyValues = Lists.newArrayList();
                localScanner.next(keyValues);
                if (keyValues.isEmpty()) {
                	// need to do something here...
                    localStart = regionStart;
                    lookBehindExhausted=true;
                    return;
                }
                Result behindResult = new Result(keyValues);
                byte[] startKey = boundary.getStartKey(behindResult);
                if (startKey == null) {
                    localStart = regionStart;
                    lookBehindExhausted=true;
                    return;
                }
                	
                if(Bytes.compareTo(startKey,regionStart)>=0){
                    //the key starts entirely in this region, so we don't need
                    //to worry about lookbehinds or skipping ahead
                    localStart = regionStart;
                    lookBehindExhausted=true;
                }else if(boundary.shouldLookBehind(startKey)){
                    localStart = regionStart;
                    remoteStart = startKey;
                }else if(boundary.shouldStartLate(startKey)){
                    localStart = boundary.getStopKey(behindResult);
                    lookBehindExhausted=true;
                }
            }finally{
                if(localScanner!=null)localScanner.close();
            }
        }
    }

    private Filter getCorrectFilter(Filter filter, String transactionId) {
        if(!SpliceUtils.NA_TRANSACTION_ID.equals(transactionId)|| filter==null){
           return filter;
        }
        /*
         * If we have no transaction id, we need to make sure and remove the SI Filter from the list,
         * because otherwise shit'll break
         */
        if(filter instanceof SIFilter) return null;
        else if(filter instanceof FilterList){
            FilterList list = (FilterList)filter;
            FilterList copy = new FilterList();
            boolean added = false;
            for(Filter listedFilter:list.getFilters()){
                if(!(listedFilter instanceof SIFilter)){
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

    public Scan toScan() {
        //this is naive--we should probably pay attention to look-behinds and look-aheads here
        Scan retScan = boundary.buildScan(transactionId,scan.getStartRow(),scan.getStopRow());
        retScan.setFilter(scan.getFilter());
        return retScan;
    }

    public byte[] getTableName() {
        return tableName;
    }
}
