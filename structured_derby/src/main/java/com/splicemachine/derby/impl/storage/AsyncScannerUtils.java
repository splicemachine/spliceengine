package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.derby.impl.job.operation.SuccessFilter;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.hbase.async.*;

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
        scanner.setMaxVersions(scan.getMaxVersions());

        Map<String,byte[]> attributesMap = scan.getAttributesMap();
        if(attributesMap!=null && attributesMap.size()>0){
            scanner.setFilter(new AsyncAttributeHolder(attributesMap));
        }
        Filter f= scan.getFilter();
        if(f instanceof FilterList){
            FilterList oldFl = (FilterList)f;
            List<ScanFilter> scanFilters = Lists.newArrayListWithCapacity(oldFl.getFilters().size());
            for(Filter subFilter:oldFl.getFilters()){
                scanFilters.add(convertFilter(subFilter));
            }
            org.hbase.async.FilterList newFl = new org.hbase.async.FilterList(scanFilters);
            scanner.setFilter(newFl);
        }else if (f!=null){
            scanner.setFilter(convertFilter(f));
        }
        //TODO -sf- set scan filters
//        scanner.setFilter(scan.getFilter());
        return scanner;
    }

    private static ScanFilter convertFilter(Filter filter) {
        if(filter instanceof SuccessFilter){
            SuccessFilter sf = (SuccessFilter)filter;
            return new AsyncSuccessFilter(sf.getTaskList());
        }

        throw new IllegalArgumentException("Unknown Filter of type "+ filter.getClass());
    }

    public static List<KeyValue> convertFromAsync(List<org.hbase.async.KeyValue> kvs){
        return Lists.transform(kvs,toHBaseKvFunction);
    }
}
