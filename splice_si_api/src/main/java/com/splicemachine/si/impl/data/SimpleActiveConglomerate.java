package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.ActiveConglomerate;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 * Created by jleach on 12/20/16.
 */
public class SimpleActiveConglomerate implements ActiveConglomerate {
    @Override
    public long getTransactionID1() {
        return 0;
    }

    @Override
    public long getVersion() {
        return 0;
    }

    @Override
    public boolean hasTombstone() {
        return false;
    }

    @Override
    public long getTransactionID2() {
        return 0;
    }

    @Override
    public long getEffectiveTimestamp() {
        return 0;
    }

    @Override
    public int numberOfColumns() {
        return 0;
    }

    @Override
    public UnsafeRow getData() {
        return null;
    }
}
