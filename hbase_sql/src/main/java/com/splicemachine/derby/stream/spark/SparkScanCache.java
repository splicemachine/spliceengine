package com.splicemachine.derby.stream.spark;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import org.apache.spark.api.java.JavaPairRDD;

import java.util.concurrent.TimeUnit;

public class SparkScanCache {
    public static Cache<String, JavaPairRDD<RowLocation, ExecRow>> cache =
            CacheBuilder.newBuilder().concurrencyLevel(8).maximumSize(1024)
                    .expireAfterAccess(5, TimeUnit.MINUTES)
                    .removalListener(l -> ((JavaPairRDD)l.getValue()).unpersist(false)).build();
}
