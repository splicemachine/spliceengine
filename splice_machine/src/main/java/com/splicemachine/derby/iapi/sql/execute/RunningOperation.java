package com.splicemachine.derby.iapi.sql.execute;

import java.util.Date;
import java.util.UUID;

import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 *  Entity for storing running operations' info
 *  (Operation, Thread, Submitted Time, Running Engine)
 *
 *  @author Changli Liu
 *  @time 01/29/2018
 */

public class RunningOperation{

    private final UUID uuid;
    private final String rdbIntTkn;
    private Date submittedTime = null;
    private DataSetProcessor.Type engine = null;
    private SpliceOperation operation;
    private Thread thread;

    @SuppressFBWarnings("EI_EXPOSE_REP2") // mutable Date problem
    public RunningOperation(SpliceOperation operation,
                            Thread thread,
                            Date submittedTime,
                            DataSetProcessor.Type engine,
                            UUID uuid, String rdbIntTkn) {
        this.operation = operation;
        this.thread =thread;
        // EI_EXPOSE_REP2: May expose internal representation by incorporating
        // reference to mutable object
        // todo(martinrupp) replace java.util.Date with java.time.Instance, see
        // https://stackoverflow.com/questions/36639154/convert-java-util-date-to-what-java-time-type
        this.submittedTime = submittedTime;
        this.engine = engine;
        this.uuid = uuid;
        this.rdbIntTkn = rdbIntTkn;
    }

    @SuppressFBWarnings({"EI_EXPOSE_REP"}) // mutable Date problem
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

    public UUID getUuid() {
        return uuid;
    }

    public String getRdbIntTkn() {
        return rdbIntTkn;
    }

    public String getEngineName() {
        String scopeName = getOperation().getScopeName();
        if (scopeName.compareTo("Call Procedure") == 0) {
            return "SYSTEM";
        }
        else {
            return (getEngine() == DataSetProcessor.Type.SPARK) ? "OLAP" : "OLTP";
        }
    }
}