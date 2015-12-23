package com.splicemachine.pipeline.mem;

import com.splicemachine.access.api.WrongPartitionException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MWrongPartition extends IOException implements WrongPartitionException{

    public MWrongPartition(){
    }

    public MWrongPartition(String message){
        super(message);
    }

    public MWrongPartition(String message,Throwable cause){
        super(message,cause);
    }

    public MWrongPartition(Throwable cause){
        super(cause);
    }
}
