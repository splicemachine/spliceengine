package com.splicemachine.si.impl;

import org.apache.hadoop.hbase.DoNotRetryIOException;

public class HMissedSplit extends DoNotRetryIOException {
    public HMissedSplit(String message){
        super(message);
    }
}
