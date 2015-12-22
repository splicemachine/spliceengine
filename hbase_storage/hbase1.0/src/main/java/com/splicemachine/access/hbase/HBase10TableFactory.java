package com.splicemachine.access.hbase;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.access.api.STableFactory;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 * Created by jleach on 11/18/15.
 */
public class HBase10TableFactory implements STableFactory<TableName>{
    protected Connection connection;
    protected HBaseTableInfoFactory hbaseTableInfoFactory;
    private static STableFactory<TableName> INSTANCE=new HBase10TableFactory();

    public HBase10TableFactory(){
        try{
            connection=HBaseConnectionFactory.getInstance().getConnection();
            hbaseTableInfoFactory=HBaseTableInfoFactory.getInstance();
        }catch(IOException ioe){
            throw new RuntimeException(ioe);
        }
    }

    public static STableFactory<TableName> getInstance(){
        return INSTANCE;
    }

    @Override
    public Partition getTable(TableName tableName) throws IOException{
        return new ClientPartition(connection.getTable(tableName));
    }

    @Override
    public Partition getTable(String name) throws IOException{
        return new ClientPartition(connection.getTable(hbaseTableInfoFactory.getTableInfo(name)));
    }

    public List<HRegionLocation> getRegions(byte[] tableName) throws IOException, ExecutionException, InterruptedException{
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getAllRegionLocations();
    }

    public List<HRegionLocation> getRegions(String tableName,boolean refresh) throws IOException, ExecutionException, InterruptedException{
        if(refresh)
            clearRegionCache(hbaseTableInfoFactory.getTableInfo(tableName));
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getAllRegionLocations();
    }

    public void clearRegionCache(TableName tableName){
        ((HConnection)connection).clearRegionCache(tableName);
    }


    public List<HRegionLocation> getRegionsInRange(byte[] tableName,final byte[] startRow,final byte[] stopRow) throws IOException, ExecutionException, InterruptedException{
        List<HRegionLocation> locations=getRegions(tableName);
        if(startRow.length<=0 && stopRow.length<=0)
            return locations;             //short circuit in the case where all regions are contained
        return Lists.newArrayList(Iterables.filter(locations,new Predicate<HRegionLocation>(){
            @Override
            public boolean apply(@Nullable HRegionLocation hRegionLocation){
                assert hRegionLocation!=null;
                return BaseHRegionUtil.containsRange(hRegionLocation.getRegionInfo(),startRow,stopRow);
            }
        }));
    }

    public HRegionLocation getRegionInRange(byte[] tableName,final byte[] startRow) throws IOException, ExecutionException, InterruptedException{
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getRegionLocation(startRow);
    }

    public Table getRawTable(TableName tableName) throws IOException{
        return connection.getTable(tableName);
    }
}
