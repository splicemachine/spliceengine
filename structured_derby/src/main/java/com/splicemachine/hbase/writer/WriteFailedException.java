package com.splicemachine.hbase.writer;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

import javax.annotation.Nullable;
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

    public static WriteFailedException create(Collection<Throwable> errors){
        return new WriteFailedException(Collections2.transform(errors,new Function<Throwable, String>() {
            @Override
            public String apply(@Nullable Throwable input) {
                return input.getMessage();
            }
        }));
    }

    private static String parseIntoErrorMessage(Collection<String> errorMessages) {
        StringBuilder sb = new StringBuilder("Failed ").append(errorMessages.size()).append(" writes:");
        for(String errorMessage:errorMessages){
            sb.append(errorMessage);
            sb.append(",");
            break;
        }
        sb.append(" + ").append(errorMessages.size() - 1).append(" additional errors");
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
