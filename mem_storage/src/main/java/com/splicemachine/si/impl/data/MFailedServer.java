package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.server.FailedServerException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class MFailedServer extends IOException implements FailedServerException{
    public MFailedServer(){
    }

    public MFailedServer(String message){
        super(message);
    }

    public MFailedServer(String message,Throwable cause){
        super(message,cause);
    }

    public MFailedServer(Throwable cause){
        super(cause);
    }
}
