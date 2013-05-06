package com.splicemachine.hbase;

import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.Row;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class WriteFailedException extends IOException {


    public WriteFailedException(Collection<String> errorMessages){
        super(parseIntoErrorMessage(errorMessages));
    }

    private static String parseIntoErrorMessage(Collection<String> errorMessages) {
        StringBuilder sb = new StringBuilder("Failed ").append(errorMessages.size()).append(" writes:");
        boolean isFirst=true;
        for(String errorMessage:errorMessages){
            if(!isFirst) sb = sb.append(",");
            else isFirst=false;
            sb = sb.append(errorMessage);
        }
        return sb.toString();
    }


}
