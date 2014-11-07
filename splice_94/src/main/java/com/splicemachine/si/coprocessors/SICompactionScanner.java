package com.splicemachine.si.coprocessors;

import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.SICompactionState;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import java.io.IOException;
import java.util.List;

/**
 * Decorator for an HBase scanner that performs SI operations at compaction time. Delegates the core work to
 * SICompactionState.
 */
public class SICompactionScanner extends BaseSICompactionScanner<KeyValue,Put,Delete,Get, Scan> {
    
    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner, SDataLib<KeyValue,Put,Delete,Get,Scan> dataLib) {
    	super(compactionState,scanner,dataLib);
    }

    @Override
    public boolean next(List<KeyValue> results) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> results, String metric) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit) throws IOException {
        return nextDirect(results, limit);
    }

    @Override
    public boolean next(List<KeyValue> results, int limit, String metric) throws IOException {
        return nextDirect(results, limit);
    }

}