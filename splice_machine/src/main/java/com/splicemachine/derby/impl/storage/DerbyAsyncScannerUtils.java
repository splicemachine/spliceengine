package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.async.*;
import com.splicemachine.derby.impl.job.operation.BaseSuccessFilter;
import com.splicemachine.hbase.AbstractSkippingScanFilter;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Utilities for using asyncHbase within Splice.  See related class SIAsyncUtils.
 *
 * @author Scott Fines
 * Date: 7/15/14
 */
public class DerbyAsyncScannerUtils {

    public static List<Scanner> convertScanners(List<Scan> scans,
                                                byte[] table,
                                                HBaseClient hBaseClient,
                                                boolean populateBlockCache){
        List<Scanner> scanner = new ArrayList<>(scans.size());
        for(int i=0;i<scans.size();i++){
            scanner.add(convertScanner(scans.get(i),table,hBaseClient,populateBlockCache));
        }
        return scanner;
    }

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient){
        return convertScanner(scan, table, hbaseClient, true);
    }

    public static Scanner convertScanner(Scan scan, byte[] table, HBaseClient hbaseClient,boolean populateBlockCache){
        Scanner scanner = hbaseClient.newScanner(table);
        scanner.setStartKey(scan.getStartRow());
        scanner.setMaxNumKeyValues(scan.getBatch());
        byte[] stop = scan.getStopRow();
        if(stop.length>0)
            scanner.setStopKey(stop);
        scanner.setServerBlockCache(populateBlockCache);
        scanner.setMaxVersions(scan.getMaxVersions());
        int caching = scan.getCaching();
        if(caching>0)
            scanner.setMaxNumRows(caching);

        Filter f= scan.getFilter();
        List<ScanFilter> scanFilters = Lists.newArrayListWithExpectedSize(1);
        if(f instanceof FilterList){
            FilterList oldFl = (FilterList)f;
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
        }else if(filter instanceof AbstractSkippingScanFilter){
        	AbstractSkippingScanFilter sf = (AbstractSkippingScanFilter)filter;
            return new AsyncSkippingScanFilter(sf);
        }

        throw new IllegalArgumentException("Unknown Filter of type "+ filter.getClass());
    }

    /**
     * Returns a Function that converts Scans to Scanners using the specified table and client.
     */
    public static Function<Scan, Scanner> convertFunction(byte[] table, HBaseClient hbaseClient) {
        return new ConvertFunction(table, hbaseClient);
    }


    private static class ConvertFunction implements Function<Scan, Scanner> {
        private byte[] tableName;
        private HBaseClient hbaseClient;

        private ConvertFunction(byte[] tableName, HBaseClient hbaseClient) {
            this.tableName = tableName;
            this.hbaseClient = hbaseClient;
        }

        @Override
        public Scanner apply(Scan scan) {
            return convertScanner(scan, tableName, hbaseClient);
        }
    }

}
