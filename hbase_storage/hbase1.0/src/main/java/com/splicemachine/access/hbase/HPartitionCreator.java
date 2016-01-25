package com.splicemachine.access.hbase;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.ClientPartition;
import com.splicemachine.storage.Partition;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
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
    private final Clock clock;
    private final HBaseTableInfoFactory tableInfoFactory;

    public HPartitionCreator(HBaseTableInfoFactory tableInfoFactory,Connection connection,Clock clock,HColumnDescriptor userDataFamilyDescriptor){
        this.connection = connection;
        this.userDataFamilyDescriptor = userDataFamilyDescriptor;
        this.tableInfoFactory = tableInfoFactory;
        this.clock = clock;
    }

    @Override
    public PartitionCreator withName(String name){
        descriptor = new HTableDescriptor(tableInfoFactory.getTableInfo(name));
        return this;
    }

    @Override
    public PartitionCreator withCoprocessor(String coprocessor) throws IOException{
        assert descriptor!=null: "Programmer error: must specify name first!";
        descriptor.addCoprocessor(coprocessor);
        return this;
    }

    @Override
    public Partition create() throws IOException{
        assert descriptor!=null: "No table to create!";
        descriptor.addFamily(userDataFamilyDescriptor);
        try(Admin admin = connection.getAdmin()){
            admin.createTable(descriptor);
        }
        TableName tableName=descriptor.getTableName();
        return new ClientPartition(connection,tableName,connection.getTable(tableName),clock);
    }
}
