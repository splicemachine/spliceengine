package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionCreator;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class HPartitionCreator implements PartitionCreator{
    private HTableDescriptor descriptor;
    private final Connection connection;
    private final HColumnDescriptor userDataFamilyDescriptor;

    public HPartitionCreator(Connection connection,HColumnDescriptor userDataFamilyDescriptor){
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
        try(Admin admin = connection.getAdmin()){
            admin.createTable(descriptor);
        }
    }
}
