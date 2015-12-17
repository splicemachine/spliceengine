package com.splicemachine.si.impl.data;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MNoSuchFamily extends IOException{
    public MNoSuchFamily(){
    }

    public MNoSuchFamily(String message){
        super(message);
    }

    public MNoSuchFamily(String message,Throwable cause){
        super(message,cause);
    }

    public MNoSuchFamily(Throwable cause){
        super(cause);
    }
}
