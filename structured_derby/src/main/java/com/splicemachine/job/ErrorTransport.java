package com.splicemachine.job;

import com.splicemachine.derby.utils.Exceptions;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.InvocationTargetException;

/**
 * Utility class for transporting Error information from machine to machine in
 * an efficient manner.
 *
 * Note that a stack trace is NOT carried over, only the message itself
 *
 * @author Scott Fines
 * Created on: 9/19/13
 */
public class ErrorTransport implements Externalizable{

    private String messageId;
    private boolean shouldRetry;
    private boolean isStandard;
    private Object[] args;

    public ErrorTransport() {
    }

    private ErrorTransport( String messageId,
                          boolean isStandardException,
                          boolean shouldRetry, Object[] args) {
        this.shouldRetry = shouldRetry;
        this.messageId = messageId;
        this.args = args;
        this.isStandard = isStandardException;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(messageId);
        out.writeBoolean(isStandard);
        out.writeBoolean(shouldRetry);
        out.writeInt(args.length);
        for(Object o:args){
            out.writeObject(o);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        messageId = in.readUTF();
        isStandard = in.readBoolean();
        shouldRetry = in.readBoolean();
        args = new Object[in.readInt()];
        for(int i=0;i<args.length;i++){
            args[i] = in.readObject();
        }
    }

    public StandardException getStandardException(){
        return StandardException.newException(messageId,args);
    }

    public static ErrorTransport newTransport(StandardException se){
        String mId = se.getMessageId();
        Object[] args = se.getArguments();
        boolean shouldRetry = false;
        return new ErrorTransport(mId,true,shouldRetry,args);
    }

    public static ErrorTransport newTransport(Throwable t){
        t = Exceptions.getRootCause(t);
        if(t instanceof StandardException)
            return newTransport((StandardException)t);

        boolean shouldRetry = Exceptions.shouldRetry(t);
        if(!shouldRetry){
            if(t instanceof DoNotRetryIOException){
                String message = t.getMessage();
                if(message!=null && message.contains("transaction") && message.contains("is not ACTIVE. State is ERROR")){
                    shouldRetry=true;
                }
            }
        }
        String message = t.getMessage();

        return new ErrorTransport(message,false,shouldRetry,new Object[]{t.getClass().getCanonicalName()});
    }

    public Throwable getError(){
        if(isStandard) return getStandardException();

        try {
            Class<? extends Throwable> errorClazz = (Class<? extends Throwable>) Class.forName(args[0].toString());
            return errorClazz.getConstructor(String.class).newInstance((messageId != null ? messageId : "null"));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean shouldRetry() {
        return shouldRetry;
    }
}
