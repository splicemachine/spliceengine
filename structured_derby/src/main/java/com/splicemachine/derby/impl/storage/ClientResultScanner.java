package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.sql.execute.operations.DistributedScanner;
import com.splicemachine.derby.impl.sql.execute.operations.RowKeyDistributorByHashPrefix;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;
import java.util.Iterator;

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

    private HTableInterface table;

    public ClientResultScanner(byte[] tableName, Scan scan,boolean bucketed) {
        this.tableName = tableName;
        this.scan = scan;
        if(bucketed){
						SpreadBucket bucketingStrategy = SpliceDriver.driver().getTempTable().getCurrentSpread();
            keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(bucketingStrategy));
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
            scanner = table.getScanner(scan);
        else
            scanner = DistributedScanner.create(table,scan,keyDistributor);
    }

    @Override
    public Result next() throws IOException {
        return scanner.next();
    }

    @Override
    public Result[] next(int nbRows) throws IOException {
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
