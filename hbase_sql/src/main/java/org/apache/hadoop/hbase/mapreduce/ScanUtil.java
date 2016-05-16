package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Scan;

public class ScanUtil {

    public static String convert(Scan scan) throws IOException {
        return TableMapReduceUtil.convertScanToString(scan);
    }
}