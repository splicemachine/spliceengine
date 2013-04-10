package com.splicemachine.derby.impl.job.load;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import org.apache.hadoop.hbase.client.HTableInterface;

/**
 * @author Scott Fines
 *         Created on: 4/5/13
 */
public abstract class ImportJob implements CoprocessorJob {
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
}
