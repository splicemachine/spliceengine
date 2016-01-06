package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.ipc.CallerDisconnectedException;

/**
 * @author Scott Fines
 *         Date: 1/7/16
 */
public class HCallerDisconnected extends CallerDisconnectedException{
    public HCallerDisconnected(String msg){
        super(msg);
    }
}
