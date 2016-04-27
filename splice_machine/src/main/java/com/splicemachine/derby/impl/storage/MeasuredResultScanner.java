package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.HBaseStatUtils;
import com.splicemachine.metrics.*;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 1/27/14
 */
public class MeasuredResultScanner extends ReopenableScanner implements SpliceResultScanner{
    private static Logger LOG=Logger.getLogger(MeasuredResultScanner.class);
    private ResultScanner delegate;

    private Timer remoteTimer;
    private Counter remoteBytesCounter;
    private HTableInterface htable;
    private Scan scan;

    private long rowsRead=0l;

    public MeasuredResultScanner(HTableInterface htable,Scan scan,ResultScanner delegate,MetricFactory metricFactory){
        this.htable=htable;
        this.scan=scan;
        this.delegate=delegate;
        this.remoteTimer=metricFactory.newWallTimer();
        this.remoteBytesCounter=metricFactory.newCounter();
    }

    @Override
    public void open() throws IOException, StandardException{
    }

    @Override
    public TimeView getRemoteReadTime(){
        return remoteTimer.getTime();
    }

    @Override
    public long getRemoteBytesRead(){
        return remoteBytesCounter.getTotal();
    }

    @Override
    public long getRemoteRowsRead(){
        return remoteTimer.getNumEvents();
    }

    @Override
    public TimeView getLocalReadTime(){
        return Metrics.noOpTimeView();
    }

    @Override
    public long getLocalBytesRead(){
        return 0;
    }

    @Override
    public long getLocalRowsRead(){
        return 0;
    }

    @Override
    public Result next() throws IOException{
        remoteTimer.startTiming();
        Result next=null;
        try{
            next=delegate.next();
            if(next!=null && next.size()>0){
                rowsRead++;
                remoteTimer.tick(1);
                setLastRow(next.getRow());
                HBaseStatUtils.countBytes(remoteBytesCounter,next);
            }else{
                remoteTimer.stopTiming();
                if(LOG.isTraceEnabled())
                    LOG.trace("Read "+rowsRead+" rows");
            }
        }catch(IOException e){
            if(Exceptions.isScannerTimeoutException(e) && getNumRetries()<MAX_RETIRES){
                if(LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG,"Re-create scanner with startRow = %s",BytesUtil.toHex(getLastRow()));
                incrementNumRetries();
                delegate=reopenResultScanner(delegate,scan,htable);
                next=next();
            }else{
                SpliceLogUtils.logAndThrow(LOG,e);
            }
        }
        return next;
    }

    @Override
    public Result[] next(int nbRows) throws IOException{
        remoteTimer.startTiming();
        Result[] results=null;
        try{
            results=delegate.next(nbRows);
            if(results!=null && results.length>0){
                rowsRead+=results.length;
                remoteTimer.tick(results.length);
                HBaseStatUtils.countBytes(remoteBytesCounter,results);
                setLastRow(results[results.length-1].getRow());
            }else{
                remoteTimer.stopTiming();
                if(LOG.isTraceEnabled())
                    LOG.trace("Read "+rowsRead+" rows");
            }
        }catch(IOException e){
            if(Exceptions.isScannerTimeoutException(e) && getNumRetries()<MAX_RETIRES){
                byte[] lastRow=getLastRow();
                if(lastRow!=null && LOG.isTraceEnabled()){
                    SpliceLogUtils.trace(LOG,"Re-create scanner with startRow = %s",BytesUtil.toHex(lastRow));
                }

                incrementNumRetries();
                delegate=reopenResultScanner(delegate,scan,htable);
                results=delegate.next(nbRows);
            }else{
                SpliceLogUtils.logAndThrow(LOG,e);
            }
        }
        return results;
    }

    @Override
    public void close(){
        delegate.close();
    }

    @Override
    public Iterator<Result> iterator(){
        return new MeasuredIterator();
    }

    private class MeasuredIterator implements Iterator<Result>{
        Result next=null;

        @Override
        public boolean hasNext(){
            if(next==null){
                next=next();
            }
            return next!=null;
        }

        @Override
        public Result next(){
            if(next==null) throw new NoSuchElementException();
            Result n=next;
            next=null;
            return n;
        }

        @Override
        public void remove(){
            throw new UnsupportedOperationException();
        }
    }
}
