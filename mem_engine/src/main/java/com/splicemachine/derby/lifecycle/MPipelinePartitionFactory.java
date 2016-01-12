package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.Partition;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MPipelinePartitionFactory implements PartitionFactory<Object>{
    private final PartitionFactory<Object> baseFactory;

    public MPipelinePartitionFactory(PartitionFactory<Object> baseFactory){
        this.baseFactory=baseFactory;
    }

    public void initialize(Clock clock,SConfiguration configuration) throws IOException{
        baseFactory.initialize(clock,configuration);
    }

    public Partition getTable(String name) throws IOException{
        return baseFactory.getTable(name);
    }

    public PartitionAdmin getAdmin() throws IOException{
        return new MEnginePartitionAdmin(baseFactory.getAdmin());
    }

    public Partition getTable(Object tableName) throws IOException{
        return baseFactory.getTable(tableName);
    }

    public Partition getTable(byte[] name) throws IOException{
        return baseFactory.getTable(name);
    }
}
