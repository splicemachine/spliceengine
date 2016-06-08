package com.splicemachine.si.impl;

import com.splicemachine.access.api.CallTimeoutException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/8/16
 */
public class HCallTimeout extends IOException implements CallTimeoutException{
    public HCallTimeout(){
    }

    public HCallTimeout(String message){
        super(message);
    }

    public HCallTimeout(String message,Throwable cause){
        super(message,cause);
    }

    public HCallTimeout(Throwable cause){
        super(cause);
    }
}
