package com.splicemachine.hbase;

import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Created by jleach on 12/16/15.
 */
public class ReadOnlyHTableDescriptor extends HTableDescriptor {
    public ReadOnlyHTableDescriptor(HTableDescriptor desc) {
            super(desc.getTableName(), desc.getColumnFamilies(), desc.getValues());
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }
}
