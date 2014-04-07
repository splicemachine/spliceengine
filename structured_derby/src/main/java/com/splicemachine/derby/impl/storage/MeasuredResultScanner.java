package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.stats.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;
import org.apache.log4j.Logger;

/**
 * @author Scott Fines
 *         Date: 1/27/14
 */
public class MeasuredResultScanner extends ReopenableScanner implements SpliceResultScanner {
        private static Logger LOG = Logger.getLogger(MeasuredResultScanner.class);
		private ResultScanner delegate;

		private Timer remoteTimer;
		private Counter remoteBytesCounter;
        private HTableInterface htable;
        private Scan scan;

		public MeasuredResultScanner(HTableInterface htable, Scan scan, ResultScanner delegate, MetricFactory metricFactory) {
				this.htable = htable;
                this.scan = scan;
                this.delegate = delegate;
				this.remoteTimer = metricFactory.newTimer();
				this.remoteBytesCounter = metricFactory.newCounter();
		}

		@Override public void open() throws IOException, StandardException {  }

		@Override public TimeView getRemoteReadTime() { return remoteTimer.getTime(); }
		@Override public long getRemoteBytesRead() { return remoteBytesCounter.getTotal(); }
		@Override public long getRemoteRowsRead() { return remoteTimer.getNumEvents(); }
		@Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
		@Override public long getLocalBytesRead() { return 0; }
		@Override public long getLocalRowsRead() { return 0; }

		@Override
		public Result next() throws IOException {
				remoteTimer.startTiming();
                Result next = null;
                try {
                    next = delegate.next();
                    if (next != null && next.size() > 0) {
                        remoteTimer.tick(1);
                        setLastRow(next.getRow());
                        StatUtils.countBytes(remoteBytesCounter, next);
                    } else
                        remoteTimer.stopTiming();
                } catch (IOException e) {
                    if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                        SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", BytesUtil.toHex(getLastRow()));
                        incrementNumRetries();
                        delegate = reopenResultScanner(delegate, scan, htable);
                        next = next();
                    }
                    else {
                        SpliceLogUtils.logAndThrowRuntime(LOG, e);
                    }
                }
				return next;
		}

		@Override
		public Result[] next(int nbRows) throws IOException {
				remoteTimer.startTiming();
                Result[] results = null;
                try {
                    results = delegate.next(nbRows);
                    if (results != null && results.length > 0) {
                        remoteTimer.tick(results.length);
                        StatUtils.countBytes(remoteBytesCounter, results);
                        setLastRow(results[results.length-1].getRow());
                    } else
                        remoteTimer.stopTiming();
                } catch (IOException e) {
                    if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                        SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", BytesUtil.toHex(getLastRow()));
                        incrementNumRetries();
                        delegate = reopenResultScanner(delegate, scan, htable);
                        results = delegate.next(nbRows);
                    }
                    else {
                        SpliceLogUtils.logAndThrowRuntime(LOG, e);
                    }
                }
				return results;
		}

		@Override public void close() { delegate.close(); }

		@Override
		public Iterator<Result> iterator() {
				return new MeasuredIterator();
		}

		private class MeasuredIterator implements Iterator<Result> {
				Result next = null;

				@Override
				public boolean hasNext() {
						if(next==null){
							next = next();
						}
						return next !=null;
				}

				@Override
				public Result next() {
						if(next==null) throw new NoSuchElementException();
						Result n = next;
						next = null;
						return n;
				}

				@Override public void remove() { throw new UnsupportedOperationException(); }
		}
}
