package com.splicemachine.access.client;

import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class ClientRegionConstants{
    final static byte[] FLUSH = Bytes.toBytes("F");
    final static byte[] HOLD = Bytes.toBytes("H");
    public final static String SPLICE_SCAN_MEMSTORE_ONLY="MR";
    final public static KeyValue MEMSTORE_BEGIN = new KeyValue(HConstants.EMPTY_START_ROW,HOLD,HOLD);
    final public static KeyValue MEMSTORE_END = new KeyValue(HConstants.EMPTY_END_ROW,HOLD,HOLD);
    final public static KeyValue MEMSTORE_BEGIN_FLUSH = new KeyValue(HConstants.EMPTY_START_ROW,FLUSH,FLUSH);
}