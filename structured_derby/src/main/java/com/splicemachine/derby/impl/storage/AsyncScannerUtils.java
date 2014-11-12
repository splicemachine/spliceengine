package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.async.*;
import com.splicemachine.derby.impl.job.operation.BaseSuccessFilter;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

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

    private static final Function<? super com.splicemachine.async.KeyValue, ? extends org.apache.hadoop.hbase.KeyValue> toHBaseKvFunction
            = new Function<com.splicemachine.async.KeyValue, org.apache.hadoop.hbase.KeyValue>() {
        @Override
        public org.apache.hadoop.hbase.KeyValue apply(@Nullable com.splicemachine.async.KeyValue input) {
            return new org.apache.hadoop.hbase.KeyValue(input.key(),input.family(),input.qualifier(),input.timestamp(),input.value());
        }
    };

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient){
        return convertScanner(scan, table, hbaseClient, true);
    }

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient,boolean populateBlockCache){
        Scanner scanner = hbaseClient.newScanner(table);
        scanner.setStartKey(scan.getStartRow());
        byte[] stop = scan.getStopRow();
        if(stop.length>0)
            scanner.setStopKey(stop);
        scanner.setServerBlockCache(populateBlockCache);
        scanner.setMaxVersions(scan.getMaxVersions());

        Filter f= scan.getFilter();
        List<ScanFilter> scanFilters = Lists.newArrayListWithExpectedSize(1);
        if(f instanceof FilterList){
            FilterList oldFl = (FilterList)f;
//            List<ScanFilter> flScanFilters = Lists.newArrayListWithCapacity(oldFl.getFilters().size());
            for(Filter subFilter:oldFl.getFilters()){
                scanFilters.add(convertFilter(subFilter));
            }
        }else if (f!=null){
            scanFilters.add(convertFilter(f));
        }
        Map<String,byte[]> attributesMap = scan.getAttributesMap();
        if(attributesMap!=null && attributesMap.size()>0){
            scanFilters.add(new AsyncAttributeHolder(attributesMap));
        }
        switch(scanFilters.size()){
            case 0:
                break;
            case 1:
                scanner.setFilter(scanFilters.get(0));
                break;
            default:
                scanner.setFilter(new com.splicemachine.async.FilterList(scanFilters));
        }

        return scanner;
    }

    private static ScanFilter convertFilter(Filter filter) {
        if(filter instanceof BaseSuccessFilter){
        	BaseSuccessFilter sf = (BaseSuccessFilter)filter;
            return new AsyncSuccessFilter(sf.getTaskList());
        }

        throw new IllegalArgumentException("Unknown Filter of type "+ filter.getClass());
    }

    public static List<KeyValue> convertFromAsync(List<com.splicemachine.async.KeyValue> kvs){
        return Lists.transform(kvs,toHBaseKvFunction);
    }
}
