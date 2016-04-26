package com.splicemachine.pipeline.impl;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class WriteFailedException extends IOException {

    private String tableName;
    private int attemptCount;


    public WriteFailedException(){ }

    public WriteFailedException(String tableName, int attemptCount){
        this.tableName = tableName;
        this.attemptCount = attemptCount;
    }

    public WriteFailedException(String tableName, Integer attemptCount){
        this.tableName = tableName;
        this.attemptCount = attemptCount;
    }

    public WriteFailedException(String message){
        super(message);
    }

    public WriteFailedException(String message,Throwable cause){
        super(message,cause);
    }

    public WriteFailedException(Throwable cause){
        super(cause);
    }

    public String getTableName(){
        return tableName;
    }

    public int getAttemptCount(){
        return attemptCount;
    }
}
