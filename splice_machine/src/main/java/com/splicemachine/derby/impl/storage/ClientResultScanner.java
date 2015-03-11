package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Iterator;

/**
 * Uses typical HBase client to return results
 * @author Scott Fines
 * Created on: 10/30/13
 */
public class ClientResultScanner extends ReopenableScanner implements SpliceResultScanner{
    private static Logger LOG = Logger.getLogger(ClientResultScanner.class);
    private SpliceResultScanner scanner;
    private final byte[] tableName;
    private final Scan scan;
    private final RowKeyDistributor keyDistributor;
    private final MetricFactory metricFactory;
    private HTableInterface table;

		private long rowsRead = 0l;

    public ClientResultScanner(byte[] tableName,
                               Scan scan,
                               boolean bucketed,
                               MetricFactory metricFactory) {
        this(tableName, scan, bucketed, metricFactory,null);
    }

    public ClientResultScanner(byte[] tableName,
															 Scan scan,
															 boolean bucketed,
															 MetricFactory metricFactory,
                               boolean[] usedBuckets) {
				this.metricFactory = metricFactory;
        this.tableName = tableName;
        this.scan = scan;
        if(bucketed){
						SpreadBucket bucketingStrategy = SpliceDriver.driver().getTempTable().getCurrentSpread();
            RowKeyDistributor distributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(bucketingStrategy));
            if(usedBuckets!=null)
                keyDistributor = new FilteredRowKeyDistributor(distributor,usedBuckets);
            else
                keyDistributor = distributor;
				}else
            keyDistributor = null;
    }

    @Override
    public void open() throws IOException, StandardException {
        if(table==null){
            table = SpliceAccessManager.getHTable(tableName);
        }
        if(scanner!=null)
            scanner.close();
        if(keyDistributor==null)
            scanner = new MeasuredResultScanner(table,scan,table.getScanner(scan),metricFactory);
        else{
            scanner = DistributedScanner.create(table,scan,keyDistributor,metricFactory);
        }
        scanner.open();
    }

		@Override public TimeView getRemoteReadTime() { return scanner.getRemoteReadTime(); }
		@Override public long getRemoteBytesRead() { return scanner.getRemoteBytesRead(); }
		@Override public long getRemoteRowsRead() { return scanner.getRemoteRowsRead(); }

		@Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
		@Override public long getLocalBytesRead() { return 0l; }
		@Override public long getLocalRowsRead() { return 0; }

		@Override
        public Result next() throws IOException {
            Result r;
                r = scanner.next();
                if (r != null && r.size() > 0) {
										rowsRead++;
                } else {
										if(LOG.isTraceEnabled())
												LOG.trace("Read "+rowsRead+" rows");
                }
            return r;
		}

    @Override public Result[] next(int nbRows) throws IOException {
				return scanner.next(nbRows);
	}

    @Override
    public void close() {
        try{
            if(scanner!=null)
                scanner.close();
        }finally{
            if(table!=null){
                try {
                    table.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    @Override
    public Iterator<Result> iterator() {
        if(scanner!=null)
            return scanner.iterator();
        throw new AssertionError("Did not open scanner properly!");
    }
}
