package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.*;
import java.util.concurrent.TimeUnit;

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
    private final Clock clock;

    public ClientPartition(Connection connection,
                           TableName tableName,
                           Table table,
                           Clock clock){
        this.tableName=tableName;
        this.table=table;
        this.connection=connection;
        this.clock = clock;
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
    protected long doIncrement(Increment incr) throws IOException{
        return Bytes.toLong(table.increment(incr).value());
    }

    @Override
    public Iterator<DataResult> batchGet(Attributable attributes,List<byte[]> rowKeys) throws IOException{
        List<Get> gets=new ArrayList<>(rowKeys.size());
        for(byte[] rowKey : rowKeys){
            Get g=new Get(rowKey);
            if(attributes!=null){
                for(Map.Entry<String, byte[]> attr : attributes.allAttributes().entrySet()){
                    g.setAttribute(attr.getKey(),attr.getValue());
                }
            }
            gets.add(g);
        }
        Result[] results=table.get(gets);
        if(results.length<=0) return Collections.emptyIterator();
        final HResult retResult=new HResult();
        return Iterators.transform(Iterators.forArray(results),new Function<Result, DataResult>(){
            @Override
            public DataResult apply(Result input){
                retResult.set(input);
                return retResult;
            }
        });
    }

    @Override
    public boolean checkAndPut(byte[] key,byte[] family,byte[] qualifier,byte[] expectedValue,DataPut put) throws IOException{
        return false;
    }

    @Override
    public List<Partition> subPartitions(){
        try(Admin admin=connection.getAdmin()){
            //TODO -sf- does this cache region information?
            List<HRegionInfo> tableRegions=admin.getTableRegions(tableName);
            List<Partition> partitions=new ArrayList<>(tableRegions.size());
            for(HRegionInfo info : tableRegions){
                partitions.add(new RangedClientPartition(connection,tableName,table,info,new LazyPartitionServer(connection,info,tableName),clock));
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

    @Override
    public List<Partition> subPartitions(byte[] startRow,byte[] stopRow){
        return null;
    }

    @Override
    public PartitionLoad getLoad() throws IOException{
        Map<ServerName, List<HRegionInfo>> serverToRegionMap=new HashMap<>();
        try(RegionLocator rl=connection.getRegionLocator(tableName)){
            List<HRegionLocation> locations=rl.getAllRegionLocations();
            for(HRegionLocation location : locations){
                List<HRegionInfo> info=serverToRegionMap.get(location.getServerName());
                if(info==null){
                    info = new LinkedList<>();
                    serverToRegionMap.put(location.getServerName(),info);
                }
                info.add(location.getRegionInfo());
            }
        }
        int totalStoreFileSizeMB = 0;
        int totalMemstoreSieMB = 0;
        int storefileIndexSizeMB = 0;
        try(Admin admin=connection.getAdmin()){
            ClusterStatus clusterStatus=admin.getClusterStatus();
            for(Map.Entry<ServerName,List<HRegionInfo>> entry:serverToRegionMap.entrySet()){
                ServerLoad load=clusterStatus.getLoad(entry.getKey());
                Map<byte[], RegionLoad> regionsLoad=load.getRegionsLoad();
                for(HRegionInfo info:entry.getValue()){
                    RegionLoad rl = regionsLoad.get(info.getRegionName());
                    totalStoreFileSizeMB+=rl.getStorefileSizeMB();
                    totalMemstoreSieMB+=rl.getMemStoreSizeMB();
                    storefileIndexSizeMB+=rl.getStorefileIndexSizeMB();
                }
            }
        }
        return new HPartitionLoad(getName(),totalStoreFileSizeMB,totalMemstoreSieMB,storefileIndexSizeMB);
    }

    @Override
    public void compact() throws IOException{
        //TODO -sf- this is inherently synchronous
        try(Admin admin = connection.getAdmin()){
            admin.majorCompact(tableName);
            AdminProtos.GetRegionInfoResponse.CompactionState compactionState;
            do{
                try{
                    clock.sleep(500l,TimeUnit.MILLISECONDS);
                }catch(InterruptedException e){
                    throw new InterruptedIOException();
                }
                compactionState=admin.getCompactionState(tableName);
            }while(compactionState!=AdminProtos.GetRegionInfoResponse.CompactionState.NONE);
        }
    }
}
