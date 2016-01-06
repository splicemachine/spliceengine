package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.ipc.FailedServerException;


/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HFailedServer extends FailedServerException implements com.splicemachine.si.api.server.FailedServerException{
    public HFailedServer(String message){
        super(message);
    }
}
