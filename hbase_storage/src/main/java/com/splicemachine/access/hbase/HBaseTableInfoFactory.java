package com.splicemachine.access.hbase;

import com.splicemachine.access.HConfiguration;import com.splicemachine.access.api.SConfiguration;import com.splicemachine.access.api.STableInfoFactory;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.TableName;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseTableInfoFactory implements STableInfoFactory<TableName> {
    private final String namespace;
    private final byte[] namespaceBytes;

    public HBaseTableInfoFactory(SConfiguration config) {
        this.namespace = config.getString(HConfiguration.NAMESPACE);
        this.namespaceBytes = Bytes.toBytes(namespace);

    }

    @Override
    public TableName getTableInfo(String name) {
        return TableName.valueOf(namespace,name);
    }

    @Override
    public TableName getTableInfo(byte[] name) {
        return TableName.valueOf(namespaceBytes,name);
    }

}
