package com.splicemachine.storage;

import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Represents an HBase Table as a single Partition.
 *
 * @author Scott Fines
 *         Date: 12/17/15
 */
@NotThreadSafe
public class ClientPartition extends SkeletonHBaseClientPartition{
    private TableName tableName;
    private final Table table;
    private final Connection connection;

    public ClientPartition(Connection connection,TableName tableName,Table table){
        this.tableName=tableName;
        this.table=table;
        this.connection = connection;
    }

    @Override
    public String getTableName(){
        return table.getName().getQualifierAsString();
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

    @Override
    public List<Partition> subPartitions(){
        try(Admin admin = connection.getAdmin()){
            //TODO -sf- does this cache region information?
            List<HRegionInfo> tableRegions=admin.getTableRegions(tableName);
            List<Partition> partitions = new ArrayList<>(tableRegions.size());
            for(HRegionInfo info:tableRegions){
                partitions.add(new RangedClientPartition(connection,tableName,table,info,new LazyPartitionServer(connection,info,tableName)));
            }
            return partitions;
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public PartitionServer owningServer(){
        throw new UnsupportedOperationException("A Table is not owned by a single server, but by the cluster as a whole");
    }
}
