package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.ipc.CallTimeoutException;
import org.apache.hadoop.hbase.ipc.FailedServerException;

/**
 * @author Scott Fines
 *         Date: 5/13/15
 */
public class ErrorString{

    public static String getCallTimeoutString(){
        return CallTimeoutException.class.getCanonicalName();
    }

    public static String getFailedServerString(){
        return FailedServerException.class.getCanonicalName();
    }

    public static Throwable failedServer(String s){
        return new FailedServerException(s);
    }
}
