package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.api.TableDescriptor;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionServer;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MEnginePartitionAdmin implements PartitionAdmin{
    private final PartitionAdmin admin;

    public MEnginePartitionAdmin(PartitionAdmin admin){
        this.admin=admin;
    }

    @Override
    public PartitionCreator newPartition() throws IOException{
        return new PipelinePartitionCreator(admin.newPartition());
    }

    @Override
    public void deleteTable(String tableName) throws IOException{
        admin.deleteTable(tableName);
    }

    @Override
    public void splitTable(String tableName,byte[]... splitPoints) throws IOException{
        admin.splitTable(tableName, splitPoints);
    }

    @Override
    public void close() throws IOException{
        admin.close();
    }

    @Override
    public Collection<PartitionServer> allServers() throws IOException{
        return admin.allServers();
    }

    @Override
    public Iterable<? extends Partition> allPartitions(String tableName) throws IOException{
        return admin.allPartitions(tableName);
    }

    @Override
    public TableDescriptor[] getTableDescriptors(List<String> tables) throws IOException{
        return admin.getTableDescriptors(tables);
    }
}
