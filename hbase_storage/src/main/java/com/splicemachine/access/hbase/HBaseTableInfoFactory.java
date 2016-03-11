package com.splicemachine.access.hbase;

import org.apache.hadoop.hbase.TableName;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.api.STableInfoFactory;
import com.splicemachine.primitives.Bytes;

/**
 * Created by jleach on 11/18/15.
 */
public class HBaseTableInfoFactory implements STableInfoFactory<TableName> {
    private static volatile HBaseTableInfoFactory INSTANCE;
    private final String namespace;
    private final byte[] namespaceBytes;

    //use the getInstance() method instead
    private HBaseTableInfoFactory(SConfiguration config) {
        this.namespace = config.getNamespace();
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

    @Override
    public TableName parseTableInfo(String namespacePlusTable) {
        return TableName.valueOf(namespacePlusTable);
    }

    public static HBaseTableInfoFactory getInstance(SConfiguration configuration){
        HBaseTableInfoFactory htif = INSTANCE;
        if(htif==null){
            synchronized(HBaseTableInfoFactory.class){
                htif = INSTANCE;
                if(htif==null){
                    htif = INSTANCE = new HBaseTableInfoFactory(configuration);
                }
            }
        }
        return htif;
    }
}
