package com.splicemachine.hbase;

import java.io.IOException;
import java.util.Collection;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public class WriteFailedException extends IOException {


    public WriteFailedException(Collection<String> errorMessages){
        super(parseIntoErrorMessage(errorMessages));
    }

    private static String parseIntoErrorMessage(Collection<String> errorMessages) {
        StringBuilder sb = new StringBuilder("Failed ").append(errorMessages.size()).append(" writes");
        return sb.toString();
//        boolean isFirst=true;
//        for(String errorMessage:errorMessages){
//            if(!isFirst) sb = sb.append(",");
//            else isFirst=false;
//            sb = sb.append(errorMessage);
//        }
//        return sb.toString();
    }


}
