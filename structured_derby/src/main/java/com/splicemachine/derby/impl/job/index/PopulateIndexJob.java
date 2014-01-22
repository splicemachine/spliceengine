package com.splicemachine.derby.impl.job.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public class PopulateIndexJob implements CoprocessorJob{
    private final HTableInterface table;
    private final String transactionId;
    private final long indexConglomId;
    private final long baseConglomId;
    private final int[] mainColToIndexPosMap;
    private final BitSet indexedColumns;
    private final boolean isUnique;
    private final BitSet descColumns;
		private final long statementId;
		private final long operationId;
		private final String xplainSchema;

		public PopulateIndexJob(HTableInterface table,
                          String transactionId,
                          long indexConglomId,
                          long baseConglomId,
                          int[] indexColToMainColPosMap,
                          boolean unique,
                          boolean[] descColumns,
													long statementId,
													long operationId,
													String xplainSchema) {
        this.table = table;
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        isUnique = unique;
				this.statementId = statementId;
				this.operationId = operationId;
				this.xplainSchema = xplainSchema;


        this.indexedColumns = new BitSet();
        for(int indexCol:indexColToMainColPosMap){
            indexedColumns.set(indexCol-1);
        }
        this.mainColToIndexPosMap = new int[(int) indexedColumns.length()];
        for(int indexCol=0;indexCol<indexColToMainColPosMap.length;indexCol++){
            int mainCol = indexColToMainColPosMap[indexCol];
            mainColToIndexPosMap[mainCol-1] = indexCol;
        }
        this.descColumns = new BitSet(descColumns.length);
        for(int col=0;col<descColumns.length;col++){
            if(descColumns[col])
                this.descColumns.set(col);
        }
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        PopulateIndexTask task = new PopulateIndexTask(transactionId,indexConglomId,
                baseConglomId, mainColToIndexPosMap, indexedColumns,isUnique,getJobId(),descColumns,xplainSchema,statementId,operationId);
        return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "indexJob-"+transactionId.substring(transactionId.lastIndexOf('/')+1);
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override
    public TransactionId getParentTransaction() {
        return HTransactorFactory.getTransactorControl().transactionIdFromString(transactionId);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
