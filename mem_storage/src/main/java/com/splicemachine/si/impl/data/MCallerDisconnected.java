package com.splicemachine.si.impl.data;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class MCallerDisconnected extends IOException{
    public MCallerDisconnected(){
    }

    public MCallerDisconnected(String message){
        super(message);
    }

    public MCallerDisconnected(String message,Throwable cause){
        super(message,cause);
    }

    public MCallerDisconnected(Throwable cause){
        super(cause);
    }
}
