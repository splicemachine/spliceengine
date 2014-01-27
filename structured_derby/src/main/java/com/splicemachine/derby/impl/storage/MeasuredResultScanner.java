package com.splicemachine.derby.impl.storage;

import com.splicemachine.stats.*;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Scott Fines
 *         Date: 1/27/14
 */
public class MeasuredResultScanner implements SpliceResultScanner {
		private final ResultScanner delegate;

		private Timer remoteTimer;
		private Counter remoteBytesCounter;

		public MeasuredResultScanner(ResultScanner delegate, MetricFactory metricFactory) {
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
				Result next = delegate.next();
				if(next!=null && next.size()>0){
						remoteTimer.tick(1);
						StatUtils.countBytes(remoteBytesCounter, next);
				}else
						remoteTimer.stopTiming();
				return next;
		}

		@Override
		public Result[] next(int nbRows) throws IOException {
				remoteTimer.startTiming();
				Result[] results = delegate.next(nbRows);
				if(results!=null && results.length>0){
						remoteTimer.tick(results.length);
						StatUtils.countBytes(remoteBytesCounter,results);
				}else
						remoteTimer.stopTiming();
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
