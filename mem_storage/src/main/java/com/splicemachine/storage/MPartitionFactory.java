/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage;

import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.collect.Iterables;
import com.splicemachine.access.api.*;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.Bytes;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MPartitionFactory implements PartitionFactory<Object>{
    private final Map<String, Partition> partitionMap=new ConcurrentHashMap<>();

    @Override
    public void initialize(Clock clock,SConfiguration configuration,PartitionInfoCache partitionInfoCache) throws IOException{
        //no-op
    }

    @Override
    public Partition getTable(Object tableName) throws IOException{
        return getTable((String)tableName);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        Partition partition=partitionMap.get(name);
        if(partition==null) throw new IOException("Table "+name+" not found!");
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
            this.name=name;
            return this;
        }

        @Override
        public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
            //no-op
            return this;
        }

        @Override
        public PartitionCreator withDisplayNames(String[] displayNames){
            //no-op
            return this;
        }

        @Override
        public PartitionCreator withPartitionSize(long partitionSize){
            //no-op
            return this;
        }

        @Override
        public Partition create() throws IOException{
            assert name!=null:"No name specified!";
            final MPartition p=new MPartition(name,name);
            partitionMap.put(name,p);
            return p;
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

        @Override
        public void splitRegion(byte[] regionName, byte[]... splitPoints) throws IOException {
            throw new UnsupportedOperationException("Cannot split partitions in an in-memory storage engine!");
        }

        @Override
        public void close() throws IOException{
        } //no-op

        @Override
        public Collection<PartitionServer> allServers() throws IOException{
            return Collections.<PartitionServer>singletonList(new MPartitionServer());
        }

        @Override
        public Iterable<? extends Partition> allPartitions(final String tableName) throws IOException{
            if(tableName==null) return partitionMap.values();
            return Iterables.filter(partitionMap.values(), new Predicate<Partition>() {
                @Override
                public boolean apply(Partition partition) {
                    return partition.getTableName().equals(tableName);
                }
            });
        }

        @Override
        public TableDescriptor [] getTableDescriptors(List<String> tables) throws IOException{
            throw new UnsupportedOperationException("Cannot get table descriptors in an in-memory storage engine!");
        }

        @Override
        public Iterable<TableDescriptor> listTables() throws IOException {
            throw new UnsupportedOperationException("Cannot list table descriptors in an in-memory storage engine!");
        }

        @Override
        public void move(String partition, String server) throws IOException {
            throw new UnsupportedOperationException("Cannot move partitions in an in-memory storage engine!");
        }

        @Override
        public TableDescriptor getTableDescriptor(String table) throws IOException{
            throw new UnsupportedOperationException("Cannot get table descriptors in an in-memory storage engine!");
        }
    }
}
