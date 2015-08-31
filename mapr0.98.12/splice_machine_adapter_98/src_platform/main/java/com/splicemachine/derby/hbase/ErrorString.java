package com.splicemachine.derby.hbase;

import org.apache.hadoop.hbase.ipc.RpcClient;

/**
 * @author Scott Fines
 *         Date: 5/13/15
 */
public class ErrorString{

    public static String getCallTimeoutString(){
        return RpcClient.CallTimeoutException.class.getCanonicalName();
    }

    public static String getFailedServerString(){
        return RpcClient.FailedServerException.class.getCanonicalName();
    }

    public static Throwable failedServer(String s){
        return new RpcClient.FailedServerException(s);
    }
}
