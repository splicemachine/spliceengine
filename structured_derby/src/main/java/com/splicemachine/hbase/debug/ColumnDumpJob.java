package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 9/24/13
 */
public class ColumnDumpJob extends DebugJob{
    private final int columnNumber;

    public ColumnDumpJob(String destinationDirectory,
                   String tableName, int columnNumber,Configuration config) {
        super(tableName,destinationDirectory,config);
        this.opId = "columnDump:"+tableName;
        this.columnNumber = columnNumber;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        return Collections.singletonMap(new ColumnDumpTask(opId,destinationDirectory,columnNumber),
                Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }
}
