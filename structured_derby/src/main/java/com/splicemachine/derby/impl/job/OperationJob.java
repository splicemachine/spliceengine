package com.splicemachine.derby.impl.job;

import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.job.Job;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class OperationJob implements Job {
    private Scan scan;
    private SpliceObserverInstructions instructions;
    private HTableInterface table;

    @Override
    public String getJobId() {
        return instructions.getTopOperation().getUniqueSequenceID();
    }

    public Scan getScan(){
        return scan;
    }

    public SpliceObserverInstructions getInstructions(){
        return instructions;
    }

    public HTableInterface getTable(){
        return table;
    }
}
