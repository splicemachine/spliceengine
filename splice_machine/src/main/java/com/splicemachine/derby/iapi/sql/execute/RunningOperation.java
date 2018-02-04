package com.splicemachine.derby.iapi.sql.execute;

import java.util.Date;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;

/**
 *  Entity for storing running operations' info
 *  (Operation, Thread, Submitted Time, Running Engine)
 *
 *  @author Changli Liu
 *  @time 01/29/2018
 */

public class RunningOperation{

    private Date submittedTime = null;
    private DataSetProcessor.Type engine = null;
    private SpliceOperation operation;
    private Thread thread;

    public RunningOperation(SpliceOperation operation,
                                Thread thread,
                                Date submittedTime,
                                DataSetProcessor.Type engine) {
        this.operation = operation;
        this.thread =thread;
        this.submittedTime = submittedTime;
        this.engine = engine;
    }

    public Date getSubmittedTime() {
        return submittedTime;
    }

    public DataSetProcessor.Type getEngine() {
        return engine;
    }

    public SpliceOperation getOperation() {
        return operation;
    }

    public Thread getThread() {
        return thread;
    }

}
