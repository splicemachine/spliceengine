package com.splicemachine.storage;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class LazyPartitionServer implements PartitionServer{
    private final Connection connection;
    private final TableName tableName;
    private final HRegionInfo regionInfo;

    public LazyPartitionServer(Connection connection, HRegionInfo regionInfo,TableName tableName){
        this.connection=connection;
        this.tableName = tableName;
        this.regionInfo=regionInfo;
    }

    @Override
    public int compareTo(PartitionServer o){
       return 0;
    }
}
