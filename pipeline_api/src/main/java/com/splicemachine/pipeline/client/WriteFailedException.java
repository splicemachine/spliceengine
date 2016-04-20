package com.splicemachine.pipeline.client;

import java.io.IOException;

/**
 * Exception to indicate that a write was failed.
 *
 * @author Scott Fines
 *         Date: 4/19/16
 */
public class WriteFailedException extends IOException{

    private String tableName;
    private int attemptCount;


    public WriteFailedException(String tableName,int attemptCount){
        this.tableName = tableName;
        this.attemptCount=attemptCount;
    }

    public WriteFailedException(){ }

    public WriteFailedException(String message){
        super(message);
    }

    public WriteFailedException(String message,Throwable cause){
        super(message,cause);
    }

    public WriteFailedException(Throwable cause){
        super(cause);
    }

    public String getDestinationTableName(){
        return tableName;
    }

    public int getNumAttempts(){
        return attemptCount;
    }
}
