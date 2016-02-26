package com.splicemachine.access.hbase;

import com.splicemachine.access.api.TableDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;

/**
 * Created by jyuan on 3/3/16.
 */
public class HBaseTableDescriptor implements TableDescriptor{

    HTableDescriptor hTableDescriptor;

    public HBaseTableDescriptor(HTableDescriptor hTableDescriptor) {
        this.hTableDescriptor = hTableDescriptor;
    }

    @Override
    public String getTableName() {
        return hTableDescriptor.getNameAsString();
    }

    public HTableDescriptor getHTableDescriptor() {
        return hTableDescriptor;
    }
}
