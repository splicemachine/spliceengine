package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public class SubmittedResult implements OlapResult{
    private static final long serialVersionUID = 1l;
    private long tickTime;

    public SubmittedResult(){
    }

    public SubmittedResult(long tickTime){
        this.tickTime=tickTime;
    }

    public long getTickTime(){
        return tickTime;
    }

    @Override public boolean isSuccess(){ return false; }

    @Override
    public Throwable getThrowable(){
        return null;
    }
}
