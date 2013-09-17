package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class ScanJob extends DebugJob {
    private final EntryPredicateFilter predicateFilter;

    public ScanJob(String destinationDirectory, String tableName, EntryPredicateFilter predicateFilter,Configuration config) {
        super(tableName,destinationDirectory,config);
        this.predicateFilter = predicateFilter;
        this.opId = "scan:"+tableName;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        return Collections.singletonMap(new ScanTask(opId, predicateFilter, destinationDirectory),
                Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }
}
