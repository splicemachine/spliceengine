package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.data.hbase.TransactorFactory;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public abstract class ImportJob implements CoprocessorJob {
    protected static final int importTaskPriority;
    private static final int DEFAULT_IMPORT_TASK_PRIORITY = 3;

    static{
        importTaskPriority = SpliceUtils.config.getInt("splice.task.importTaskPriority",DEFAULT_IMPORT_TASK_PRIORITY);
    }

    protected ImportContext context;
    private final HTableInterface table;
    private final String jobId;

    protected ImportJob(HTableInterface table, ImportContext context) {
        this.table = table;
        this.context = context;
        this.jobId = "import-"+context.getTableName()+"-"+context.getFilePath().getName();
    }

    @Override
    public HTableInterface getTable() {
        return table;
    }

    @Override
    public String getJobId() {
        return jobId;
    }

    @Override
    public TransactionId getParentTransaction() {
        return TransactorFactory.getDefaultTransactor().transactionIdFromString(context.getTransactionId());
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
