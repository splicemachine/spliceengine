package com.splicemachine.storage;

import org.apache.hadoop.hbase.client.*;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.List;

/**
 * Represents an HBase Table as a single Partition.
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@NotThreadSafe
public class ClientPartition extends SkeletonHBaseClientPartition{
    private final Table table;

    public ClientPartition(Table table){
        this.table=table;
    }

    @Override
    public String getTableName(){
        return table.getName().getNameAsString();
    }

    @Override
    public void close() throws IOException{
        table.close();
    }

    @Override
    protected Result doGet(Get get) throws IOException{
        return table.get(get);
    }

    @Override
    protected ResultScanner getScanner(Scan scan) throws IOException{
        return table.getScanner(scan);
    }

    @Override
    protected void doDelete(Delete delete) throws IOException{
        table.delete(delete);
    }

    @Override
    protected void doPut(Put put) throws IOException{
        table.put(put);
    }

    @Override
    protected void doPut(List<Put> puts) throws IOException{
        table.put(puts);
    }

    @Override
    protected void doIncrement(Increment incr) throws IOException{
        table.increment(incr);
    }
}
