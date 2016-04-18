package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class HMissedSplitException extends DoNotRetryIOException {
    public HMissedSplitException(String message){
        super(message);
    }
}
