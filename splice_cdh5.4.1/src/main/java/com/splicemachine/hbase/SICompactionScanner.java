package com.splicemachine.hbase;

import com.splicemachine.si.coprocessors.BaseSICompactionScanner;
import com.splicemachine.si.impl.server.SICompactionState;
import org.apache.hadoop.hbase.Cell;
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
public class SICompactionScanner extends BaseSICompactionScanner {
    
    public SICompactionScanner(SICompactionState compactionState,
                               InternalScanner scanner) {
    	super(compactionState,scanner);
    }

    @Override
    public boolean next(List<Cell> results) throws IOException {
        return nextDirect(results, -1);
    }

    @Override
    public boolean next(List<Cell> results, int limit) throws IOException {
        return nextDirect(results, limit);
    }


}