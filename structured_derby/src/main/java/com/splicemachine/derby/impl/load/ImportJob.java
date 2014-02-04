package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public abstract class ImportJob implements CoprocessorJob {

    protected ImportContext context;
    protected final HTableInterface table;
    protected final String jobId;
		protected final long statementId;
		protected final long operationId;

    protected ImportJob(HTableInterface table, ImportContext context,
												long statementId,long operationId) {
        this.table = table;
        this.context = context;
        this.jobId = "import-"+context.getTableName()+"-"+context.getFilePath().getName();
				this.statementId = statementId;
				this.operationId = operationId;
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
        return HTransactorFactory.getTransactorControl().transactionIdFromString(context.getTransactionId());
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }
}
