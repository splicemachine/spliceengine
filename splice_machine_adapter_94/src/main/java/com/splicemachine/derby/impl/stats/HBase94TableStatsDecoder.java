package com.splicemachine.derby.impl.stats;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.storage.EntryDecoder;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

/**
 * @author Scott Fines
 *         Date: 3/31/15
 */
public class HBase94TableStatsDecoder extends TableStatsDecoder{
    @Override
    protected void setKeyInDecoder(Result result,MultiFieldDecoder cachedDecoder){
        KeyValue kv = result.raw()[0];
        cachedDecoder.set(kv.getBuffer(),kv.getRowOffset(),kv.getRowLength());
    }

    @Override
    protected void setRowInDecoder(Result result,EntryDecoder cachedDecoder){
        for(KeyValue kv:result.raw()){
            if(KeyValueUtils.singleMatchingColumn(kv,SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES)){
                cachedDecoder.set(kv.getBuffer(),kv.getValueOffset(),kv.getValueLength());
                return;
            }
        }
    }
}
