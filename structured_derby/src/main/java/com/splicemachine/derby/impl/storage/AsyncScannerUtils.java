package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.hbase.async.AsyncAttributeHolder;
import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

/**
 * Utilities for using asyncHbase within Splice.
 *
 * @author Scott Fines
 * Date: 7/15/14
 */
public class AsyncScannerUtils {

    private static final Function<? super org.hbase.async.KeyValue, ? extends org.apache.hadoop.hbase.KeyValue> toHBaseKvFunction
            = new Function<org.hbase.async.KeyValue, org.apache.hadoop.hbase.KeyValue>() {
        @Override
        public org.apache.hadoop.hbase.KeyValue apply(@Nullable org.hbase.async.KeyValue input) {
            return new org.apache.hadoop.hbase.KeyValue(input.key(),input.family(),input.qualifier(),input.timestamp(),input.value());
        }
    };

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient){
        return convertScanner(scan, table, hbaseClient, true);
    }

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient,boolean populateBlockCache){
        Scanner scanner = hbaseClient.newScanner(table);
        scanner.setStartKey(scan.getStartRow());
        scanner.setStopKey(scan.getStopRow());
        scanner.setServerBlockCache(populateBlockCache);

        Map<String,byte[]> attributesMap = scan.getAttributesMap();
        if(attributesMap!=null && attributesMap.size()>0){
            scanner.setFilter(new AsyncAttributeHolder(attributesMap));
        }
        //TODO -sf- set scan filters
//        scanner.setFilter(scan.getFilter());
        return scanner;
    }

    public static List<KeyValue> convertFromAsync(List<org.hbase.async.KeyValue> kvs){
        return Lists.transform(kvs,toHBaseKvFunction);
    }
}
