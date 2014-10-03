package com.splicemachine.async;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;

import javax.annotation.Nullable;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/29/14
 */
public class AsyncScannerUtils {

    private static final Function<? super com.splicemachine.async.KeyValue, ? extends KeyValue> toHBaseKvFunction
            = new Function<com.splicemachine.async.KeyValue, org.apache.hadoop.hbase.KeyValue>() {
        @Override
        public org.apache.hadoop.hbase.KeyValue apply(@Nullable com.splicemachine.async.KeyValue input) {
            return new org.apache.hadoop.hbase.KeyValue(input.key(),input.family(),input.qualifier(),input.timestamp(),input.value());
        }
    };

    public static List<KeyValue> convertFromAsync(List<com.splicemachine.async.KeyValue> kvs){
        return Lists.transform(kvs, toHBaseKvFunction);
    }
}
