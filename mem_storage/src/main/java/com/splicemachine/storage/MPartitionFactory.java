/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage;

import com.splicemachine.db.iapi.error.StandardException;
import splice.com.google.common.base.Predicate;
import splice.com.google.common.collect.Iterables;
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
        public PartitionCreator withTransactionId(long txnId) throws IOException {
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
        public PartitionCreator withSplitKeys(byte[][] splitKeys) {
            //no-op
            return this;
        }

        @Override
        public PartitionCreator withCatalogVersion(String version) {
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
        public void mergeRegions(String regionName1, String regionName2) throws IOException {
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

        @Override
        public void snapshot(String snapshotName, String tableName) throws IOException {
            throw new UnsupportedOperationException("Cannot take a snapshot in an in-memory storage engine!");
        }

        @Override
        public void deleteSnapshot(String snapshotName) throws IOException {
            throw new UnsupportedOperationException("Cannot delete a snapshot in an in-memory storage engine!");
        }

        @Override
        public void restoreSnapshot(String snapshotName) throws IOException {
            throw new UnsupportedOperationException("Cannot restore a snapshot in an in-memory storage engine!");
        }

        @Override
        public void disableTable(String tableName) throws IOException {}

        @Override
        public void enableTable(String tableName) throws IOException {}

        @Override
        public void closeRegion(Partition partition) throws IOException, InterruptedException {}

        @Override
        public void assign(Partition partition) throws IOException, InterruptedException {}

        @Override
        public boolean tableExists(String tableName) throws IOException {
            return partitionMap.containsKey(tableName);
        }

        @Override
        public List<byte[]> hbaseOperation(String table, String operation, byte[] bytes) throws IOException {
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");
        }

        @Override
        public void markDropped(long conglomId, long txn) throws IOException {
            // do nothing
        }

        @Override
        public void enableTableReplication(String tableName) throws IOException {
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");
        }

        @Override
        public void disableTableReplication(String tableName) throws IOException {
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");

        }

        @Override
        public List<ReplicationPeerDescription> getReplicationPeers() throws IOException{
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");

        }
        
        @Override
        public boolean replicationEnabled(String tableName) throws IOException {
            return false;
        }

        @Override
        public void setCatalogVersion(long conglomerateNumber, String version) throws IOException {
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");
        }

        @Override
        public String getCatalogVersion(long conglomerateNumber) throws StandardException {
            throw new UnsupportedOperationException("Operation not supported in mem storage engine");
        }
    }
}
