package com.splicemachine.derby.iapi.sql.olap;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public interface OlapStatus{
    enum State{
        NOT_SUBMITTED,
        SUBMITTED,
        RUNNING,
        CANCELED{
            @Override public boolean isFinal(){ return true; }
        },
        FAILED{
            @Override public boolean isFinal(){ return true; }
        },
        COMPLETE{
            @Override public boolean isFinal(){ return true; }
        };

        public boolean isFinal(){ return false; }
    }

    State checkState();

    OlapResult getResult();

    void cancel();

    boolean isAvailable();

    boolean markSubmitted();

    void markCompleted(OlapResult result);

    boolean markRunning();

    boolean isRunning();
}
