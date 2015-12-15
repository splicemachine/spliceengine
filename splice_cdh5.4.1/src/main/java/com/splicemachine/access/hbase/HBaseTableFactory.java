package com.splicemachine.access.hbase;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.splicemachine.access.api.STableFactory;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.regionserver.BaseHRegionUtil;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseTableFactory implements STableFactory<TableName> {
    protected Connection connection;
    protected HBaseTableInfoFactory hbaseTableInfoFactory;
    private static HBaseTableFactory INSTANCE = new HBaseTableFactory();

    protected HBaseTableFactory() {
        try {
            connection = HBaseConnectionFactory.getInstance().getConnection();
            hbaseTableInfoFactory = HBaseTableInfoFactory.getInstance();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static HBaseTableFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public Table getTable(TableName tableName) throws IOException {
            return connection.getTable(tableName);
    }

    @Override
    public Table getTable(String name) throws IOException {
        return connection.getTable(hbaseTableInfoFactory.getTableInfo(name));
    }

    public List<HRegionLocation> getRegions(byte[] tableName) throws IOException, ExecutionException, InterruptedException {
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getAllRegionLocations();
    }

    public List<HRegionLocation> getRegions(String tableName, boolean refresh) throws IOException, ExecutionException, InterruptedException {
        if (refresh)
            clearRegionCache(hbaseTableInfoFactory.getTableInfo(tableName));
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getAllRegionLocations();
    }
    public void clearRegionCache(TableName tableName) {
        ((HConnection) connection).clearRegionCache(tableName);
    }


    public List<HRegionLocation> getRegionsInRange(byte[] tableName, final byte[] startRow, final byte[] stopRow) throws IOException, ExecutionException, InterruptedException  {
        List<HRegionLocation> locations = getRegions(tableName);
        if (startRow.length <= 0 && stopRow.length <= 0)
            return locations;             //short circuit in the case where all regions are contained
        return Lists.newArrayList(Iterables.filter(locations, new Predicate<HRegionLocation>() {
            @Override
            public boolean apply(@Nullable HRegionLocation hRegionLocation) {
                return BaseHRegionUtil.containsRange(hRegionLocation.getRegionInfo(), startRow, stopRow);
            }
        }));
    }

    public HRegionLocation getRegionInRange(byte[] tableName, final byte[] startRow) throws IOException, ExecutionException, InterruptedException  {
        return connection.getRegionLocator(hbaseTableInfoFactory.getTableInfo(tableName)).getRegionLocation(startRow);
    }

}
