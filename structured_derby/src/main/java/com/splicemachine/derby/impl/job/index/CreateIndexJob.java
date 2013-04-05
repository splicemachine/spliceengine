package com.splicemachine.derby.impl.job.index;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class CreateIndexJob implements CoprocessorJob{
    private final HTableInterface table;
    private final String transactionId;
    private final long indexConglomId;
    private final long baseConglomId;
    private final int[] indexColsToBaseColMap;
    private final boolean isUnique;

    public CreateIndexJob(HTableInterface table,
                          String transactionId,
                          long indexConglomId,
                          long baseConglomId,
                          int[] indexColsToBaseColMap,
                          boolean unique) {
        this.table = table;
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.indexColsToBaseColMap = indexColsToBaseColMap;
        isUnique = unique;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        CreateIndexTask task = new CreateIndexTask(transactionId,indexConglomId,
                baseConglomId,indexColsToBaseColMap,isUnique);

        return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "indexJob-"+transactionId;
    }
}
