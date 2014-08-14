package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Bytes;

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

    private TxnView txn;

    protected ImportJob(HTableInterface table, ImportContext context,
												long statementId,long operationId,
                        TxnView txn) {
        this.table = table;
        this.context = context;
        this.jobId = "import-"+context.getTableName()+"-"+context.getFilePath().getName();
				this.statementId = statementId;
				this.operationId = operationId;
        this.txn = txn;
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
		public byte[] getDestinationTable() {
				return context.getTableName().getBytes();
		}

    @Override
    public TxnView getTxn() {
        return txn;
    }
}
