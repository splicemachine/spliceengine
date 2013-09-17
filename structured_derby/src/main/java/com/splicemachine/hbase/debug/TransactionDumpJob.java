package com.splicemachine.hbase.debug;

import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Pair;

import java.util.Collections;
import java.util.Map;

/**
 * @author Scott Fines
 *         Created on: 9/17/13
 */
public class TransactionDumpJob extends DebugJob{
    protected TransactionDumpJob( String destinationDirectory, Configuration config) {
        super("SPLICE_TXN", destinationDirectory, config);
        this.opId="transactionDump";
    }

    @Override
    public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
        return Collections.singletonMap(new TransactionDump(opId,destinationDirectory),
                Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
    }
}
