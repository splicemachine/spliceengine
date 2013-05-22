package com.splicemachine.derby.utils;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.splicemachine.derby.error.SpliceDoNotRetryIOException;
import com.splicemachine.derby.error.SpliceIOException;
import com.splicemachine.derby.error.SpliceStandardException;
import com.splicemachine.derby.error.SpliceStandardLogUtils;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.impl.sql.execute.index.IndexNotSetUpException;
import com.splicemachine.hbase.MutationResponse;
import com.splicemachine.hbase.MutationResult;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.IOException;
import java.lang.reflect.GenericArrayType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {
    private static final Gson gson;
    private static final String OPEN_BRACE = "{";
    private static final String CLOSE_BRACE="}";

    static{
        GsonBuilder gsonBuilder = new GsonBuilder().registerTypeAdapter(StandardException.class,new StandardExceptionAdapter());
        gson = gsonBuilder.create();
    }

    private static final String COLON = ":";

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e){
        Throwable rootCause = Throwables.getRootCause(e);
        if(rootCause instanceof StandardException) return (StandardException)rootCause;

        if(rootCause instanceof ConstraintViolation.PrimaryKeyViolation){
            return StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT);
        }else if (rootCause instanceof ConstraintViolation.UniqueConstraintViolation){
            return StandardException.newException(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT);
        }else if(rootCause instanceof LangFormatException){
            LangFormatException lfe = (LangFormatException)rootCause;
            return StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION,lfe.getMessage());
        }else if (rootCause instanceof RetriesExhaustedWithDetailsException){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)rootCause;
            List<Throwable> causes = rewde.getCauses();
            //unwrap and throw any constraint violation errors
            for(Throwable t:causes){
                if(t instanceof DoNotRetryIOException) return parseException(t);
            }
        }else if(rootCause instanceof SpliceDoNotRetryIOException){
            SpliceDoNotRetryIOException spliceException = (SpliceDoNotRetryIOException)rootCause;
            String fullMessage = spliceException.getMessage();
            int firstColonIndex = fullMessage.indexOf(COLON);
            int openBraceIndex = fullMessage.indexOf(OPEN_BRACE);
            String exceptionType;
            if(firstColonIndex < openBraceIndex)
                exceptionType = fullMessage.substring(firstColonIndex+1,openBraceIndex).trim();
            else
                exceptionType = fullMessage.substring(0,openBraceIndex).trim();
            Class exceptionClass;
            try {
                exceptionClass= Class.forName(exceptionType);
            } catch (ClassNotFoundException e1) {
                //shouldn't happen, but if it does, we'll just use Exception.class and deal with the fact
                //that it might not be 100% correct
                exceptionClass = Exception.class;
            }
            String json = fullMessage.substring(openBraceIndex,fullMessage.indexOf(CLOSE_BRACE)+1);
            return parseException((Exception)gson.fromJson(json,exceptionClass));
        }else if(rootCause instanceof SpliceStandardException){
            return ((SpliceStandardException)rootCause).generateStandardException();
        }

        return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,rootCause);
    }

    public static IOException getIOException(StandardException se){
        return new SpliceDoNotRetryIOException(se.getClass().getCanonicalName()+gson.toJson(se));
    }

    public static IOException getIOException(Throwable t){
        if(t instanceof StandardException) return getIOException((StandardException)t);
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    /**
     * Determine if we should dump a stack trace to the log file.
     *
     * This is to filter out exceptions from the log that don't need to write an error to the
     * log (Primary Key violations, or other user errors, or something that can be retried without
     * punishment).
     *
     * @param e the exception to check
     * @return true if the stack trace should be logged.
     */
    public static boolean shouldLogStackTrace(Throwable e) {
        if(e instanceof ConstraintViolation.PrimaryKeyViolation) return false;
        if(e instanceof ConstraintViolation.UniqueConstraintViolation) return false;
        if(e instanceof IndexNotSetUpException) return false;
        return true;
    }

    public static Throwable fromString(String s) {
        MutationResult.Code writeErrorCode = MutationResult.Code.parse(s);
        if(writeErrorCode!=null){
            if(writeErrorCode== MutationResult.Code.WRITE_CONFLICT)
                return new WriteConflict(s);
            else if(writeErrorCode==MutationResult.Code.FAILED)
                return new DoNotRetryIOException(s);
            else return Constraints.constraintViolation(writeErrorCode);
        }
        return new DoNotRetryIOException(s);
    }

    public static class LangFormatException extends DoNotRetryIOException{
        public LangFormatException() { }
        public LangFormatException(String message) { super(message); }
        public LangFormatException(String message, Throwable cause) { super(message, cause); }
    }

    private static class StandardExceptionAdapter extends TypeAdapter<StandardException>{

        @Override
        public void write(JsonWriter out, StandardException value) throws IOException {
            out.beginObject();

            out.name("severity").value(value.getErrorCode());
            out.name("textMessage").value(value.getTextMessage());
            out.name("sqlState").value(value.getSqlState());
            out.name("messageId").value(value.getMessageId());

            out.name("arguments");
            Object[] args = value.getArguments();
            if(args==null)
                out.nullValue();
            else{
                out.beginArray();
                for(Object o: value.getArguments()){
                    if(o instanceof String){
                        out.value((String)o);
                    }
                }
                out.endArray();
            }

            //TODO -sf- add a Throwable-only constructor to StandardException
//            Throwable cause = value.getCause();
//            if(cause==null||cause == value)
//                out.name("cause").nullValue();
//            else{
//                out.name("cause");
//                gson.toJson(cause, cause.getClass(), out);
//            }
            out.endObject();
        }

        @Override
        public StandardException read(JsonReader in) throws IOException {
            in.beginObject();

            int severity = 0;
            String textMessage = null;
            String sqlState = null;
            String messageId = null;
            List<String> objectStrings = null;
            while(in.peek()!=JsonToken.END_OBJECT){
                String nextName = in.nextName();
                if("severity".equals(nextName))
                        severity = in.nextInt();
                else if("textMessage".equals(nextName))
                    textMessage = in.nextString();
                else if("sqlState".equals(nextName))
                    sqlState = in.nextString();
                else if("messageId".equals(nextName))
                    messageId = in.nextString();
                else if("arguments".equals(nextName)){
                    if(in.peek()!=JsonToken.NULL){
                        in.beginArray();
                        objectStrings = new ArrayList<String>();
                        while(in.peek()!=JsonToken.END_ARRAY){
                            objectStrings.add(in.nextString());
                        }
                        in.endArray();
                    }else
                        in.nextNull();
                }
            }

//            Throwable cause;
//            in.nextName();
//            if(in.peek()== JsonToken.NULL)
//                in.nextNull();
//            else
//                cause = gson.fromJson(in,Throwable.class);
            in.endObject();

            StandardException se;
            if(objectStrings!=null){
                Object[] objects = new Object[objectStrings.size()];
                objectStrings.toArray(objects);
                se = StandardException.newException(messageId,objects);
            }else
                se = StandardException.newException(messageId);
            se.setSeverity(severity);
            se.setSqlState(sqlState);
            se.setTextMessage(textMessage);

            return se;
        }
    }
}
