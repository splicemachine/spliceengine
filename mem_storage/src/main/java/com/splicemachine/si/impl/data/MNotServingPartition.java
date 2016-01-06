package com.splicemachine.si.impl.data;

import com.splicemachine.access.api.NotServingPartitionException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MNotServingPartition extends IOException implements NotServingPartitionException{

    public MNotServingPartition(){ }

    public MNotServingPartition(String message){
        super(message);
    }

    public MNotServingPartition(String message,Throwable cause){
        super(message,cause);
    }

    public MNotServingPartition(Throwable cause){
        super(cause);
    }
}
