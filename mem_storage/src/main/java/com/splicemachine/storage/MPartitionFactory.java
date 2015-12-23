package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.TxnPartition;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPartitionFactory implements PartitionFactory<Object>{
    private final Map<String,Partition> partitionMap = new ConcurrentHashMap<>();

    @Override
    public Partition getTable(Object tableName) throws IOException{
        return getTable((String)tableName);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        Partition partition=partitionMap.get(name);
        if(partition==null) throw new IOException("Table "+ name+" not found!");
        return partition;
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(Bytes.toString(name));
    }

    @Override
    public void createPartition(String name) throws IOException{
        partitionMap.put(name,new MPartition(name,name));
    }
}
