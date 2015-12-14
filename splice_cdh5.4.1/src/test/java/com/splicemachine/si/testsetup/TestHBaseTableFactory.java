package com.splicemachine.si.testsetup;

import com.google.common.base.Preconditions;
import com.splicemachine.access.hbase.HBaseTableFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class TestHBaseTableFactory extends HBaseTableFactory {

    private final Map<String, HTable> hTables = new HashMap<>();

    private final HBaseTestingUtility testCluster;
    private final String[] families;

    private final ExecutorService tableExecutorService = Executors.newCachedThreadPool();

    public TestHBaseTableFactory(HBaseTestingUtility testCluster, String[] families) {
        this.testCluster = testCluster;
        this.families = families;

    }

    public void addTable(HBaseTestingUtility testCluster, String tableName, String[] families) {
        byte[][] familyBytes = getFamilyBytes(families);
        try {
            hTables.put(tableName, testCluster.createTable(Bytes.toBytes(tableName), familyBytes, Integer.MAX_VALUE));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void addPackedTable(String tableName) throws IOException {
        byte[][] familyBytes = getFamilyBytes(families);

        Preconditions.checkArgument(!hTables.containsKey(tableName), "Table already exists");
        HTableDescriptor descriptor = new HTableDescriptor(TableName.valueOf(tableName));
        descriptor.addCoprocessor(SIObserver.class.getName());
        for (byte[] familyName : familyBytes) {
            HColumnDescriptor family = new HColumnDescriptor(familyName);
            family.setInMemory(true);
            descriptor.addFamily(family);
        }
        testCluster.getHBaseAdmin().createTable(descriptor);
    }

    @Override
    public Table getTable(String tableName) throws IOException {
        return new HTable(new Configuration(testCluster.getConfiguration()), Bytes.toBytes(tableName), tableExecutorService);
    }

    protected byte[][] getFamilyBytes(String[] families) {
        byte[][] familyBytes = new byte[families.length][];
        int i = 0;
        for (String f : families) {
            familyBytes[i] = Bytes.toBytes(f);
            i++;
        }
        return familyBytes;
    }


    @Override
    public Table getTable(TableName tableName) throws IOException {
        return getTable(tableName.getNameAsString());
    }
}
