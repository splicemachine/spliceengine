package com.splicemachine.derby.impl.storage;

import java.io.IOException;
import java.util.Iterator;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.RowKeyDistributorByHashPrefix;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.stats.Counter;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;

/**
 * Uses typical HBase client to return results
 * @author Scott Fines
 * Created on: 10/30/13
 */
public class ClientResultScanner implements SpliceResultScanner{
    private ResultScanner scanner;
    private final byte[] tableName;
    private final Scan scan;
    private final RowKeyDistributorByHashPrefix keyDistributor;
		private final MetricFactory metricFactory;

    private HTableInterface table;

		private final Timer remoteReadTimer;
		private final Counter remoteBytesRead;

    public ClientResultScanner(byte[] tableName,
															 Scan scan,
															 boolean bucketed,
															 MetricFactory metricFactory) {
				this.metricFactory = metricFactory;
        this.tableName = tableName;
        this.scan = scan;
        if(bucketed){
						SpreadBucket bucketingStrategy = SpliceDriver.driver().getTempTable().getCurrentSpread();
            keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(bucketingStrategy));
				}else
            keyDistributor = null;
				this.remoteReadTimer = metricFactory.newTimer();
				this.remoteBytesRead = metricFactory.newCounter();
    }

    @Override
    public void open() throws IOException, StandardException {
        if(table==null){
            table = SpliceAccessManager.getHTable(tableName);
        }
        if(scanner!=null)
            scanner.close();
        if(keyDistributor==null)
            scanner = table.getScanner(scan);
        else
            scanner = DistributedScanner.create(table,scan,keyDistributor,metricFactory);
    }

		@Override public TimeView getRemoteReadTime() { return remoteReadTimer.getTime(); }
		@Override public long getRemoteBytesRead() { return remoteBytesRead.getTotal(); }
		@Override public long getRemoteRowsRead() { return remoteReadTimer.getNumEvents(); }

		@Override public TimeView getLocalReadTime() { return Metrics.noOpTimeView(); }
		@Override public long getLocalBytesRead() { return 0l; }
		@Override public long getLocalRowsRead() { return 0; }

		@Override public Result next() throws IOException {
				remoteReadTimer.startTiming();
				Result r = scanner.next();
				if(r!=null&&r.size()>0){
						remoteReadTimer.tick(1);
						if(remoteBytesRead.isActive()){
							for(Cell kv:r.rawCells()){
								remoteBytesRead.add(kv.getRowArray().length);
							}
						}
				}else{
						remoteReadTimer.tick(0);
				}
				return r;
		}

    @Override public Result[] next(int nbRows) throws IOException {
				remoteReadTimer.startTiming();
				Result[] results = scanner.next(nbRows);
				if(results!=null && results.length>0){
						remoteReadTimer.tick(results.length);
						if(remoteBytesRead.isActive()){
								for(Result r:results){
										for(Cell kv:r.rawCells()){
												remoteBytesRead.add(kv.getRowArray().length);
										}
								}
						}
				}else
						remoteReadTimer.tick(0);
				return results;
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
