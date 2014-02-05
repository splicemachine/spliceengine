package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.impl.sql.execute.operations.AbstractRowKeyDistributor;
import com.splicemachine.stats.*;
import com.splicemachine.stats.util.Folders;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Interface for client-side scanning the data written with keys distribution
 * 
 * @author Alex Baranau
 */
public class DistributedScanner implements SpliceResultScanner {
    private final AbstractRowKeyDistributor keyDistributor;
    private final SpliceResultScanner[] scanners;
    private final List<Result>[] nextOfScanners;
    private Result next = null;

    @SuppressWarnings("unchecked")
    public DistributedScanner(AbstractRowKeyDistributor keyDistributor, SpliceResultScanner[] scanners) throws IOException {
        this.keyDistributor = keyDistributor;
        this.scanners = scanners;
        this.nextOfScanners = new List[scanners.length];
        for (int i = 0; i < this.nextOfScanners.length; i++) {
            this.nextOfScanners[i] = new ArrayList<Result>();
        }
    }

		@Override public void open() throws IOException, StandardException {  }

		@Override
		public TimeView getRemoteReadTime() {
				MultiTimeView tView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
				for(SpliceResultScanner scanner:scanners){
						tView.update(scanner.getRemoteReadTime());
				}
				return tView;
		}

		@Override
		public long getRemoteBytesRead() {
				long totalBytes =0l;
				for(SpliceResultScanner scanner:scanners){
						totalBytes+=scanner.getRemoteBytesRead();
				}
				return totalBytes;
		}

		@Override
		public long getRemoteRowsRead() {
				long totalRows =0l;
				for(SpliceResultScanner scanner:scanners){
						totalRows+=scanner.getRemoteRowsRead();
				}
				return totalRows;
		}

		@Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
		@Override public long getLocalBytesRead() { return 0; }
		@Override public long getLocalRowsRead() { return 0; }

		@Override
    public Result next() throws IOException {
        if (hasNext(1)) {
            Result toReturn = next;
            next = null;
            return toReturn;
        }

        return null;
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
        // Identical to HTable.ClientScanner implementation
        // Collect values to be returned here
        ArrayList<Result> resultSets = new ArrayList<Result>(nbRows);
        for (int i = 0; i < nbRows; i++) {
            Result next = next();
            if (next != null) {
                resultSets.add(next);
            } else {
                break;
            }
        }
        return resultSets.toArray(new Result[resultSets.size()]);
    }

    @Override
    public void close() {
        for (int i = 0; i < scanners.length; i++) {
            scanners[i].close();
        }
    }

    public static DistributedScanner create(HTableInterface hTable, Scan originalScan,
            AbstractRowKeyDistributor keyDistributor,MetricFactory metricFactory) throws IOException {
        Scan[] scans = keyDistributor.getDistributedScans(originalScan);

        SpliceResultScanner[] rss = new SpliceResultScanner[scans.length];
        for (int i = 0; i < scans.length; i++) {
            rss[i] = new MeasuredResultScanner(hTable.getScanner(scans[i]),metricFactory);
        }

        return new DistributedScanner(keyDistributor, rss);
    }

    private Result nextInternal(int nbRows) throws IOException {
        Result result = null;
        int indexOfScannerToUse = -1;
        for (int i = 0; i < nextOfScanners.length; i++) {
            if (nextOfScanners[i] == null) {
                // result scanner is exhausted, don't advance it any more
                continue;
            }

            if (nextOfScanners[i].size() == 0) {
                // advancing result scanner
                Result[] results = scanners[i].next(nbRows);
                if (results.length == 0) {
                    // marking result scanner as exhausted
                    nextOfScanners[i] = null;
                    continue;
                }
                nextOfScanners[i].addAll(Arrays.asList(results));
            }

            // if result is null or next record has original key less than the
            // candidate to be returned
            if (result == null
                    || Bytes.compareTo(keyDistributor.getOriginalKey(nextOfScanners[i].get(0).getRow()),
                            keyDistributor.getOriginalKey(result.getRow())) < 0) {
                result = nextOfScanners[i].get(0);
                indexOfScannerToUse = i;
            }
        }

        if (indexOfScannerToUse >= 0) {
            nextOfScanners[indexOfScannerToUse].remove(0);
        }

        return result;
    }

    @Override
    public Iterator<Result> iterator() {
        // Identical to HTable.ClientScanner implementation
        return new Iterator<Result>() {
            // The next RowResult, possibly pre-read
            Result next = null;

            // return true if there is another item pending, false if there
            // isn't.
            // this method is where the actual advancing takes place, but you
            // need
            // to call next() to consume it. hasNext() will only advance if
            // there
            // isn't a pending next().
            public boolean hasNext() {
                if (next == null) {
                    try {
                        next = DistributedScanner.this.next();
                        return next != null;
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
                return true;
            }

            // get the pending next item and advance the iterator. returns null
            // if
            // there is no next item.
            public Result next() {
                // since hasNext() does the real advancing, we call this to
                // determine
                // if there is a next before proceeding.
                if (!hasNext()) {
                    return null;
                }

                // if we get to here, then hasNext() has given us an item to
                // return.
                // we want to return the item and then null out the next
                // pointer, so
                // we use a temporary variable.
                Result temp = next;
                next = null;
                return temp;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

		private boolean hasNext(int nbRows) throws IOException {
				if (next != null) {
						return true;
				}

				next = nextInternal(nbRows);

				return next != null;
		}
}