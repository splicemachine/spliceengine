package com.splicemachine.derby.impl.job.AlterTable;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.derby.catalog.UUID;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/6/14
 * Time: 9:53 AM
 * To change this template use File | Settings | File Templates.
 */
public class LoadConglomerateJob implements CoprocessorJob {

    private final HTableInterface table;
    private final UUID tableId;
    private final long fromConglomId;
    private final long toConglomId;
    private final ColumnInfo[] columnInfo;
    private final int droppedColumnPosition;
    private final String txnId;
    private final long statementId;
    private final long operationId;
    private final String xplainSchema;

    public LoadConglomerateJob(HTableInterface table,
                               UUID tableId,
                               long fromConglomId,
                               long conglomId,
                               ColumnInfo[] columnInfo,
                               int droppedColumnPosition,
                               String txnId,
                               long statementId,
                               long operationId,
                               String xplainSchema) {
        this.table = table;
        this.tableId = tableId;
        this.fromConglomId = fromConglomId;
        this.toConglomId =  conglomId;
        this.columnInfo = columnInfo;
        this.droppedColumnPosition = droppedColumnPosition;
        this.txnId = txnId;
        this.statementId = statementId;
        this.operationId = operationId;
        this.xplainSchema = xplainSchema;
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        LoadConglomerateTask task = new LoadConglomerateTask(tableId, fromConglomId, toConglomId, columnInfo, droppedColumnPosition, txnId, getJobId(), xplainSchema, statementId, operationId);
        return Collections.singletonMap(task, Pair.newPair(HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW));
    }
    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return "loadConglomerateJob-"+txnId.substring(txnId.lastIndexOf('/')+1);
    }

    @Override
    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
    }

    @Override
    public TransactionId getParentTransaction() {
        return HTransactorFactory.getTransactionManager().transactionIdFromString(txnId);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
