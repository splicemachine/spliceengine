package com.splicemachine.olap;

import java.util.concurrent.CancellationException;

/**
 * @author Scott Fines
 *         Date: 4/1/16
 */
public class CancelledResult extends AbstractOlapResult{
    @Override
    public Throwable getThrowable(){
        return new CancellationException();
    }


}
