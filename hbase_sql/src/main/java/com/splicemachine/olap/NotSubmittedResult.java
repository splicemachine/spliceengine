package com.splicemachine.olap;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;

/**
 * @author Scott Fines
 *         Date: 4/4/16
 */
public class NotSubmittedResult implements OlapResult{
    @Override public Throwable getThrowable(){ return null; }
    @Override public boolean isSuccess(){ return false; }
}
