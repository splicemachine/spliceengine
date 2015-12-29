package com.splicemachine.storage;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.access.hbase.HBaseTableInfoFactory;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class H98PartitionCreator implements PartitionCreator{
    private HTableDescriptor descriptor;
    private final HConnection connection;
    private final HColumnDescriptor userDataFamilyDescriptor;

    public H98PartitionCreator(HConnection connection,HColumnDescriptor userDataFamilyDescriptor){
        this.connection = connection;
        this.userDataFamilyDescriptor = userDataFamilyDescriptor;
    }

    @Override
    public PartitionCreator withName(String name){
        descriptor = new HTableDescriptor(HBaseTableInfoFactory.getInstance().getTableInfo(name));
        return this;
    }

    @Override
    public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
        assert descriptor!=null: "Programmer error: must specify name first!";
        descriptor.addCoprocessor(coprocessor);
        return this;
    }

    @Override
    public void create() throws IOException{
        assert descriptor!=null: "No table to create!";
        descriptor.addFamily(userDataFamilyDescriptor);
        try(HBaseAdmin admin = new HBaseAdmin(connection)){
            admin.createTable(descriptor);
        }
    }
}

