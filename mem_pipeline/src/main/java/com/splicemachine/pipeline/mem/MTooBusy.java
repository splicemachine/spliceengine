package com.splicemachine.pipeline.mem;

import com.splicemachine.pipeline.api.PipelineTooBusy;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MTooBusy extends IOException implements PipelineTooBusy{
    public MTooBusy(){
    }

    public MTooBusy(String message){
        super(message);
    }

    public MTooBusy(String message,Throwable cause){
        super(message,cause);
    }

    public MTooBusy(Throwable cause){
        super(cause);
    }
}
