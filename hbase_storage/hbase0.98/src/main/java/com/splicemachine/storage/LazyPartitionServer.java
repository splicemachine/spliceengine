package com.splicemachine.storage;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HConnection;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class LazyPartitionServer implements PartitionServer{
    private final HConnection connection;
    private final TableName tableName;
    private final HRegionInfo regionInfo;

    public LazyPartitionServer(HConnection connection, HRegionInfo regionInfo,TableName tableName){
        this.connection=connection;
        this.tableName = tableName;
        this.regionInfo=regionInfo;
    }

    @Override
    public int compareTo(PartitionServer o){
        return 0;
    }

    @Override
    public int hashCode(){
        return regionInfo.hashCode(); //actually should be the region server itself
    }

    @Override
    public boolean equals(Object obj){
        if(obj==this) return true;
        else if(!(obj instanceof PartitionServer)) return false;
        else return true;
    }
}

