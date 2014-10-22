package com.splicemachine.hbase.backup;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.Txn;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class PurgeTransactionsJob implements CoprocessorJob {
	    private final Txn backupTxn;
	    private final HTableInterface table;
    private final long backupTimestamp;

    public PurgeTransactionsJob(Txn backupTxn, long backupTimestamp, HTableInterface table) {
	        this.table = table;
	        this.backupTxn = backupTxn;
            this.backupTimestamp = backupTimestamp;
	    }

	    @Override
	    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
	    	PurgeTransactionsTask task = new PurgeTransactionsTask(backupTimestamp, getJobId());
	        return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
	    }

	    @Override
	    public HTableInterface getTable() {
	        return table;
	    }

	    @Override
	    public String getJobId() {
	        return "purgeTransactionsJob-"+backupTxn.getBeginTimestamp();
	    }

	    @Override
	    public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
	        return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
	    }

    @Override
    public byte[] getDestinationTable() {
        return table.getTableName();
    }

    @Override
    public Txn getTxn() {
        return backupTxn;
    }
	}