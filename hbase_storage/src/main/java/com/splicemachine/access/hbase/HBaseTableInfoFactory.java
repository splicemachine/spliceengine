package com.splicemachine.access.hbase;

import com.splicemachine.access.api.STableInfoFactory;
import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.TableName;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseTableInfoFactory implements STableInfoFactory<TableName> {

    private static HBaseTableInfoFactory INSTANCE = new HBaseTableInfoFactory();

    private HBaseTableInfoFactory() {

    }

    public static HBaseTableInfoFactory getInstance() {
        return INSTANCE;
    }

    @Override
    public TableName getTableInfo(String name) {
        return TableName.valueOf(SpliceConstants.spliceNamespace,name);
    }

    @Override
    public TableName getTableInfo(byte[] name) {
        return TableName.valueOf(SpliceConstants.spliceNamespaceBytes,name);
    }

}
