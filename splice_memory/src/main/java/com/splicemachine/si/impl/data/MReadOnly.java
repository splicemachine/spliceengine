package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.ReadOnlyModificationException;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MReadOnly extends IOException implements ReadOnlyModificationException{
    public MReadOnly(){
    }

    public MReadOnly(String message){
        super(message);
    }

    public MReadOnly(String message,Throwable cause){
        super(message,cause);
    }

    public MReadOnly(Throwable cause){
        super(cause);
    }
}
