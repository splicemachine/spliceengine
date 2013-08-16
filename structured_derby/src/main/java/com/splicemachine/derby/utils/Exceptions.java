package com.splicemachine.derby.utils;

import com.google.common.base.Throwables;
import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import com.splicemachine.derby.error.SpliceDoNotRetryIOException;
import com.splicemachine.derby.error.SpliceStandardException;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintViolation;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraints;
import com.splicemachine.derby.impl.sql.execute.index.IndexNotSetUpException;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.writer.WriteResult;
import com.splicemachine.si.impl.WriteConflict;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.shared.common.reference.SQLState;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

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

        if(rootCause instanceof RetriesExhaustedWithDetailsException){
            return parseException((RetriesExhaustedWithDetailsException)rootCause);
        }else if(rootCause instanceof ConstraintViolation.PrimaryKeyViolation
                || rootCause instanceof ConstraintViolation.UniqueConstraintViolation){
            return createStandardExceptionForConstraintError(SQLState.LANG_DUPLICATE_KEY_CONSTRAINT, (ConstraintViolation.ConstraintViolationException) e);
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
        }else if(rootCause instanceof RetriesExhaustedWithDetailsException){
            return parseException((RetriesExhaustedWithDetailsException)rootCause);
        }

        ErrorState state = ErrorState.stateFor(rootCause);
        return StandardException.newException(state.getSqlState(),rootCause);
    }

    public static StandardException parseException(RetriesExhaustedWithDetailsException rewde){
        List<Throwable> causes = rewde.getCauses();
        /*
         * Look for any exception that can be converted into a known error type (instead of a DATA_UNEXPECTED_EXCEPTION)
         */
        for(Throwable t:causes){
            if(isExpected(t))
                return parseException(t);
        }
        if(causes.size()>0)
            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,causes.get(0));
        else
            return StandardException.newException(SQLState.DATA_UNEXPECTED_EXCEPTION,rewde);

    }

    private static boolean isExpected(Throwable rootCause){
        ErrorState state = ErrorState.stateFor(rootCause);
        return state != ErrorState.DATA_UNEXPECTED_EXCEPTION;
    }

    public static StandardException createStandardExceptionForConstraintError(String errorCode, ConstraintViolation.ConstraintViolationException e){

        ConstraintContext cc = e.getConstraintContext();
        StandardException newException = null;

        if(cc != null){
            newException = StandardException.newException(errorCode, cc.getConstraintName(), cc.getTableName());
        }else{
            newException = StandardException.newException(errorCode);
        }

        return newException;
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

    public static Throwable fromString(MutationResult result) {
//        MutationResult.Code writeErrorCode = MutationResult.Code.parse(s);

        MutationResult.Code writeErrorCode = result.getCode();

        if(writeErrorCode!=null){
            if(writeErrorCode== MutationResult.Code.WRITE_CONFLICT)
                return new WriteConflict(result.getErrorMsg());
            else if(writeErrorCode==MutationResult.Code.FAILED)
                return new DoNotRetryIOException(result.getErrorMsg());
            else return Constraints.constraintViolation(writeErrorCode, result.getConstraintContext());
        }
        return new DoNotRetryIOException(result.getErrorMsg());
    }

    public static Throwable fromString(WriteResult result) {
//        MutationResult.Code writeErrorCode = MutationResult.Code.parse(s);

        WriteResult.Code writeErrorCode = result.getCode();

        if(writeErrorCode!=null){
            if(writeErrorCode== WriteResult.Code.WRITE_CONFLICT)
                return new WriteConflict(result.getErrorMessage());
            else if(writeErrorCode==WriteResult.Code.FAILED)
                return new DoNotRetryIOException(result.getErrorMessage());
            else return Constraints.constraintViolation(writeErrorCode, result.getConstraintContext());
        }
        return new DoNotRetryIOException(result.getErrorMessage());
    }

    public static boolean shouldRetry(Throwable error) {
        if(error instanceof ConnectException){
            //we can safely retry connection refused issued
            return error.getMessage().contains("Connection refused");
        }
        return !(error instanceof StandardException) && !(error instanceof DoNotRetryIOException);
    }

    public static Throwable getRootCause(Throwable error) {
        error = Throwables.getRootCause(error);
        if(error instanceof RetriesExhaustedWithDetailsException ){
            RetriesExhaustedWithDetailsException rewde = (RetriesExhaustedWithDetailsException)error;
            List<Throwable> causes = rewde.getCauses();
            if(causes!=null&&causes.size()>0)
                error = causes.get(0);
        }
        return error;
    }

    public static Throwable getWrapperException(String errorMessage,String sqlState) {
        StandardException se =  StandardException.newException(sqlState,errorMessage);
        se.setSqlState(sqlState);
        se.setTextMessage(errorMessage);
        return se;
    }

    public static String getErrorCode(Throwable error) {
        if(error instanceof StandardException){
            return ((StandardException)error).getSqlState();
        }
        return SQLState.DATA_UNEXPECTED_EXCEPTION;
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
