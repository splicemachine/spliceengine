package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import com.splicemachine.stats.*;
import com.splicemachine.stats.util.Folders;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;

/**
 * Distributed Client Scanner
 */
public class ProbeDistributedScanner implements SpliceResultScanner {
    private final SpliceResultScanner[] scanners;
    private final List<Result>[] nextOfScanners;
    private Result next = null;

    @SuppressWarnings("unchecked")
    public ProbeDistributedScanner(SpliceResultScanner[] scanners) throws IOException {
        this.scanners = scanners;
        this.nextOfScanners = new List[scanners.length];
        for (int i = 0; i < this.nextOfScanners.length; i++) {
            this.nextOfScanners[i] = new ArrayList<Result>();
        }
    }

    @Override
    public Result next() throws IOException {
        if (hasNext(1)) {
            Result toReturn = next;
            next = null;
            return toReturn;
        }

        return null;
    }

		//no-op
		@Override public void open() throws IOException, StandardException {  }

		@Override
		public TimeView getRemoteReadTime() {
				MultiTimeView remoteView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
				for(SpliceResultScanner scanner:scanners){
						remoteView.update(scanner.getRemoteReadTime());
				}
				return remoteView;
		}

		@Override
		public long getRemoteBytesRead() {
				long totalBytes  = 0l;
				for(SpliceResultScanner resultScanner:scanners){
						totalBytes+=resultScanner.getRemoteBytesRead();
				}
				return totalBytes;
		}

		@Override
		public long getRemoteRowsRead() {
				long totalRows  = 0l;
				for(SpliceResultScanner resultScanner:scanners){
						totalRows+=resultScanner.getRemoteRowsRead();
				}
				return totalRows;
		}

		@Override
		public TimeView getLocalReadTime() {
				MultiTimeView remoteView = new SimpleMultiTimeView(Folders.sumFolder(),Folders.sumFolder(),Folders.sumFolder(),Folders.minLongFolder(),Folders.maxLongFolder());
				for(SpliceResultScanner scanner:scanners){
						remoteView.update(scanner.getLocalReadTime());
				}
				return remoteView;
		}

		@Override
		public long getLocalBytesRead() {
				long totalBytes  = 0l;
				for(SpliceResultScanner resultScanner:scanners){
						totalBytes+=resultScanner.getLocalBytesRead();
				}
				return totalBytes;
		}

		@Override
		public long getLocalRowsRead() {
				long totalRows  = 0l;
				for(SpliceResultScanner resultScanner:scanners){
						totalRows+=resultScanner.getLocalRowsRead();
				}
				return totalRows;
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

    public static ProbeDistributedScanner create(HTableInterface hTable, List<Scan> scans,MetricFactory metricFactory) throws IOException {
        SpliceResultScanner[] rss = new SpliceResultScanner[scans.size()];
        for (int i = 0; i < scans.size(); i++) {
            rss[i] = new MeasuredResultScanner(hTable.getScanner(scans.get(i)),metricFactory);
        }
        return new ProbeDistributedScanner(rss);
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
            if (result == null) {
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
                        next = ProbeDistributedScanner.this.next();
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