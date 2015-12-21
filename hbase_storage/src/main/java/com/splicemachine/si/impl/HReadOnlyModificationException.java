package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.ReadOnlyModificationException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Indicates that an attempt was made to perform a write
 * with a read-only transaction
 *
 * @author Scott Fines
 *         Date: 6/24/14
 */
public class HReadOnlyModificationException extends DoNotRetryIOException implements ReadOnlyModificationException{
    public HReadOnlyModificationException(){
    }

    public HReadOnlyModificationException(String message){
        super(message);
    }

    public HReadOnlyModificationException(String message,Throwable cause){
        super(message,cause);
    }
}

