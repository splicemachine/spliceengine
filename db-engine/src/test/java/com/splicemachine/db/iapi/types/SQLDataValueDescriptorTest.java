package com.splicemachine.db.iapi.types;

import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ItemStatistics;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Created by jleach on 8/12/16.
 */
public abstract class SQLDataValueDescriptorTest {
    protected static double RANGE_SELECTIVITY_ERRROR_BOUNDS = 100.0d;

    protected ItemStatistics serde(ItemStatistics itemStatistics) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream(4096);
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        itemStatistics.writeExternal(oos);
        oos.flush();
        baos.flush();
        byte[] foo = baos.toByteArray();
        ColumnStatisticsImpl impl = new ColumnStatisticsImpl();
        ByteArrayInputStream bais = new ByteArrayInputStream(foo);
        ObjectInputStream ois = new ObjectInputStream(bais);
        impl.readExternal(ois);
        return impl;
    }

}
