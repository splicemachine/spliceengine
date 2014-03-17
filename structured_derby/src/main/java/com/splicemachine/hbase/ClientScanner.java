package com.splicemachine.hbase;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.regionserver.RegionServerStoppedException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * a Client Scanner implementation of ResultScanner.
 *
 * This implementation is a bit clever in that it will skip through
 * regions until it either runs out of regions or runs off the end of the scan.
 * When a particular region is exhausted, it will continue on the next region in the region list.
 *
 * ClientScanners are *not* thread-safe, and should only be used on a single Thread.
 *
 * @author Scott Fines
 *
 */
class ClientScanner implements ResultScanner {
    private static final Logger CLIENT_LOG = Logger.getLogger(ClientScanner.class);
    private final List<Result> cache;
    private final int caching;
    private Scan scan;
    private boolean closed = false;
    private HRegionInfo currentRegion = null;
    private ScannerCallable callable = null;
    private long lastNext;
    private Result lastResult = null;
    private final byte[] tableName;

    private final long maxScannerResultSize;
    private final long scannerTimeout;
    private final HConnection connection;

    ClientScanner(byte[] tableName,HConnection connection,
                          final Scan scan,long maxScannerResultSize, long scannerTimeout){
        this.connection = connection;
        this.scan = scan;
        this.lastNext = System.currentTimeMillis();
        this.scannerTimeout = scannerTimeout;
        this.tableName = tableName;

        this.caching = scan.getCaching();
        this.maxScannerResultSize = maxScannerResultSize;

        this.cache = new ArrayList<Result>(caching); //only need a list as big as the cache size
    }

    public void initialize() throws IOException {
        nextScanner(caching,false);
    }

    @Override
    public Result next() throws IOException {
        if(this.closed) return null;

        //ensure that the cache has data
        fillCache();

        // get the first entry out of the cache
        if(cache.size()>0)
            return cache.remove(0);

        return null;
    }

	private void fillCache() throws IOException {
        //if there is still data in the cache, don't try and fill it up
        if(cache.size()>0) return;

        long remainingResultSize = maxScannerResultSize;
        int cacheSpace = caching;
        boolean skipFirst =false;
        Result[] values = null;
        do{
            try{
                if(skipFirst){
                        /*
                         * skipFirst is true when we have caught an error which
                         * requires us to try connecting to a Region and starting again. When
                         * that happens, we've already read that first row, so we don't want to
                         * read it again.
                         */
                    callable.setCaching(1);
                    values = connection.getRegionServerWithRetries(callable);
                    callable.setCaching(caching);
                    skipFirst=false;
                }

                    /*
                     * If the server dumps back null, we need to terminate cause there's nothing
                     * left. If it returns an empty array, however, then the region is exhausted
                     * and we need to go on to the next one
                     */
                values = connection.getRegionServerWithRetries(callable);
            }catch(DoNotRetryIOException dnre){
                if(dnre instanceof UnknownScannerException){
                    long timeout = lastNext+scannerTimeout;
                    if(timeout < System.currentTimeMillis()){
                            /*
                             * If we're out of time, wrap the exception
                             * and dump it back to the user. Otherwise, the region moved
                             * and we used the old id against the new region server,
                             * so we need to reset the scanner and try again.
                             */
                        long elapsed = System.currentTimeMillis()-lastNext;
                        DoNotRetryIOException ex = new DoNotRetryIOException(
                                elapsed+"ms passed since the last invocation, timeout is" +scannerTimeout);
                        ex.initCause(dnre);
                        throw ex;
                    }
                }else{
                    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
                    Throwable root = Throwables.getRootCause(dnre);
                        /*
                         * If there is No server for a region or a Region server stopped, then
                         * the region is moving or doing stuff, and we just need to reset
                         * the scanner and try again. Otherwise, we need to blow up cause
                         * this error isn't good.
                         */
                    if(!(root instanceof NoServerForRegionException)&&
                            !(root instanceof RegionServerStoppedException))
                        throw dnre;
                }
                //reset the scanner and try again
                if(lastResult!=null){
                    //pick up where we left off
                    this.scan.setStartRow(lastResult.getRow());
                    skipFirst=true;
                }
                this.currentRegion=null;
                continue;
            }
                /*
                 * fill up cache with whatever we got back. Of course, we
                 * may have gotten back fewer entries than the cache can hold, in
                 * which case we'll loop back over and try again.
                 */
            lastNext = System.currentTimeMillis();
            if(values!=null && values.length >0){
                for(Result rs:values){
                    cache.add(rs);
                    for(KeyValue kv:rs.raw()){
                        remainingResultSize -= kv.heapSize();
                    }
                    cacheSpace--;
                    this.lastResult = rs;
                }
            }
                /*
                 * One of two situations occur here: either we filled up the cache with this
                 * loop around, or we ran off the end of the region. If the first, then
                 * break, otherwise get a scanner for the next region and keep going
                 */
        }while(remainingResultSize>0 && cacheSpace>0 && nextScanner(cacheSpace,values==null));
    }

	private boolean nextScanner(int nbRows, boolean finished) throws IOException {
            /*
             * Gets a Scanner for the next region in the list, unless we are out of
             * regions to operate against.
             */
        if(this.callable!=null){
            //current region is exhausted, so close it up and throw it away
            this.callable.setClose();
            connection.getRegionServerWithRetries(callable);
            this.callable=null;
        }

        byte[] localStartKey;
        if(this.currentRegion!=null){
            //find the next region to hit
            byte[] endKey = currentRegion.getEndKey();
            if(endKey==null
                    || Bytes.equals(endKey, HConstants.EMPTY_BYTE_ARRAY)
                    || checkScanStopRow(endKey)
                    || finished){
                //we're at the end of the table, so no more scanners to return
                close();
                if(CLIENT_LOG.isTraceEnabled()){
                    CLIENT_LOG.trace("Finished scanning at "+ this.currentRegion);
                }
                return false;
            }
            //set the new start to be the end of the previous region
            localStartKey = endKey;
            if(CLIENT_LOG.isTraceEnabled()){
                CLIENT_LOG.trace("Advancing internal scanner to new region starting at "+
                        Bytes.toStringBinary(localStartKey)+"'");
            }
        }else{
            //we've just started, so set ourselves to the start of the scan
            localStartKey = scan.getStartRow();
        }
        try{
            callable = getScannerCallable(localStartKey,nbRows);

            connection.getRegionServerWithRetries(callable);
            this.currentRegion = callable.getHRegionInfo();
        }catch(IOException e){
            close();
            throw e;
        }
        return true;
    }

    private boolean checkScanStopRow(byte[] endKey) {
            /*
             * Makes sure that we haven't run off the end of the table, or gone past the
             * end of the scan criteria
             */
        if(scan.getStopRow().length > 0){
            byte[] stopRow = scan.getStopRow();
            return Bytes.compareTo(stopRow,0,stopRow.length,endKey,0,endKey.length)<=0;
        }
        return false;
    }

    private ScannerCallable getScannerCallable(byte[] newStartKey, int nbRows) {
        scan.setStartRow(newStartKey);
        ScannerCallable s = new ScannerCallable(connection,tableName,scan,null);
        s.setCaching(nbRows);
        return s;
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        List<Result> resultSets = new ArrayList<Result>(nbRows);
        for(int i=0;i<nbRows;i++){
            Result next = next();
            if(next() ==null) break;
            resultSets.add(next);
        }
        return resultSets.toArray(new Result[resultSets.size()]);
    }

	@Override
    public void close() {
        if(callable!=null){
            callable.setClose();
            try{
                connection.getRegionServerWithRetries(callable);
            } catch (IOException e) {
                /*
                 * We don't really want the close method to throw an error, it's a
                 * bit annoying, and probably isn't a big deal anyway. Debug it though,
                 * just in case
                 */
                CLIENT_LOG.debug("Unexpected error closing client scanner",e);
            }
        }
        closed= true;
    }

    @Override
    public Iterator<Result> iterator() {
        return new Iterator<Result>(){
            Result nextResult = null;
            @Override
            public boolean hasNext() {
                try {
                    if(nextResult==null)
                        nextResult = ClientScanner.this.next();
                    return nextResult!=null;
                } catch (IOException e) {
                    CLIENT_LOG.error("Unexpected error fetching next result",e);
                    throw new RuntimeException(e);
                }
            }

            @Override
            public Result next() {
                if(!hasNext()) throw new NoSuchElementException();
                Result tmp = nextResult;
                nextResult = null;
                return tmp;
            }

            @Override public void remove() { throw new UnsupportedOperationException(); }
        };
    }
}
