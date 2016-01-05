package com.splicemachine.access.util;

import com.google.common.collect.Maps;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.Partition;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
@NotThreadSafe
public abstract class CachedPartitionFactory<TableInfo> implements PartitionFactory<TableInfo>{
    private final Map<String,Partition> tableCache =Maps.newHashMap();
    private final PartitionFactory<TableInfo> delegate;

    public CachedPartitionFactory(PartitionFactory<TableInfo> delegate){
        this.delegate=delegate;
    }

    @Override
    public PartitionAdmin getAdmin() throws IOException{
        return delegate.getAdmin();
    }

    @Override
    public Partition getTable(TableInfo tableName) throws IOException{
        return getTable(infoAsString(tableName));
    }

    @Override
    public void initialize(Clock clock,SConfiguration configuration) throws IOException{
       delegate.initialize(clock,configuration);
    }

    @Override
    public Partition getTable(String name) throws IOException{
        Partition p = tableCache.get(name);
        if(p==null){
            p = delegate.getTable(name);
            tableCache.put(name,p);
        }
        return p;
    }

    @Override
    public Partition getTable(byte[] name) throws IOException{
        return getTable(Bytes.toString(name));
    }

    public Collection<Partition> cachedPartitions(){
        return tableCache.values();
    }

    protected abstract String infoAsString(TableInfo tableName);
}
