package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.primitives.Bytes;

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
    public PartitionAdmin getAdmin() throws IOException{
        return new Admin();
    }

    private class Creator implements PartitionCreator{
        private String name;

        @Override
        public PartitionCreator withName(String name){
            this.name = name;
            return this;
        }

        @Override
        public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
            //no-op
            return this;
        }

        @Override
        public void create() throws IOException{
            assert name!=null: "No name specified!";
            partitionMap.put(name,new MPartition(name,name));
        }
    }

    private class Admin implements PartitionAdmin{
        @Override
        public PartitionCreator newPartition() throws IOException{
            return new Creator();
        }

        @Override
        public void deleteTable(String tableName) throws IOException{
            partitionMap.remove(tableName);
        }

        @Override
        public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
            throw new UnsupportedOperationException("Cannot split partitions in an in-memory storage engine!");
        }

        @Override public void close() throws IOException{ } //no-op
    }
}
