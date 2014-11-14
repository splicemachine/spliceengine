package com.splicemachine.si.data.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.async.*;
import com.splicemachine.constants.SIConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;

/**
 * @author Scott Fines
 *         Date: 7/29/14
 */
public class SIAsyncUtils {

    public static Function<Scan,Scanner> convertFunction(final byte[] table, Scan baseScan,final HBaseClient hBaseClient){
        return new Function<Scan, Scanner>() {
            @Nullable
            @Override
            public Scanner apply(@Nullable Scan scan) {
                if(scan==null) return null;
                return convert(scan, hBaseClient, table);
            }
        };

    }

    public static Scanner convert(Scan scan, HBaseClient hBaseClient, byte[] table) {
        Scanner scanner = hBaseClient.newScanner(table);
        scanner.setStartKey(scan.getStartRow());
        byte[] stop = scan.getStopRow();
        if(stop!=null && stop.length>0)
            scanner.setStopKey(stop);
        scanner.setServerBlockCache(scan.getCacheBlocks());
        scanner.setMaxVersions(scan.getMaxVersions());
        int caching = scan.getCaching();
        if(caching>0)
            scanner.setMaxNumRows(caching);

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

    private static ScanFilter convertFilter(Filter f) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public static PutRequest convert(HBaseClient client,byte[] table, Put put) {
        List<KeyValue> keyValues = put.getFamilyMap().get(SIConstants.DEFAULT_FAMILY_BYTES);
        byte[][] qualifiers = new byte[keyValues.size()][];
        byte[][] values = new byte[keyValues.size()][];
        long ts = put.getTimeStamp();
        for(int i=0;i<keyValues.size();i++){
            qualifiers[i] = keyValues.get(i).getQualifier();
            values[i] = keyValues.get(i).getValue();
        }
        return new PutRequest(table,
                put.getRow(),
                SIConstants.DEFAULT_FAMILY_BYTES,
                qualifiers,values,ts);
    }

    public static GetRequest convert(HBaseClient client, byte[] table, Get get) {
        GetRequest request = new GetRequest(SIConstants.TRANSACTION_TABLE_BYTES,get.getRow());
        if(get.hasFamilies()){
            request = request.family(SIConstants.DEFAULT_FAMILY_BYTES);
            NavigableSet<byte[]> bytes = get.getFamilyMap().get(SIConstants.DEFAULT_FAMILY_BYTES);
            if(bytes.size()>0){
                byte[][] qualifiers = bytes.toArray(new byte[bytes.size()][]);
                request = request.qualifiers(qualifiers);
            }
        }

        return request;
    }
}