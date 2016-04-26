package com.splicemachine.job;

import com.splicemachine.pipeline.constraint.ConstraintViolation;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.impl.WriteFailedException;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.commons.lang.reflect.ConstructorUtils;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.exceptions.RegionMovedException;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

/**
 * Utility class for transporting Error information from machine to machine in an efficient manner.
 *
 * This class is a bit weird (it handles StandardException and Throwable and the same fields have different meanings
 * in each context) be sure to read and understand these limitations before using:
 *
 * <ul>
 * <li>The a stack trace is NOT carried over, only the message itself</li>
 * <li>Assumes String returned by getMessage() should be passed to one-arg String constructor on other side of wire.</li>
 * <li>Assumes one-arg String constructor cannot handle null as argument, even if that was returned by getMessage(), and so transforms null to "null"</li>
 * <li>Message string is lost if exception has no one-arg String constructor.</li>
 * <li>Cannot be used with exception classes that are not visible (private, package private, etc)</li>
 * </ul>
 *
 * @author Scott Fines
 *         Created on: 9/19/13
 */
public class ErrorTransport implements Externalizable {

    private static Logger LOG = Logger.getLogger(ErrorTransport.class);

    /* For StandardException the normal messageId, for an arbitrary Throwable, the arg to on-arg-string constructor,
     * if there is one. */
    private String messageId;

    private boolean shouldRetry;

    private boolean isStandard;

    /* For StandardException these are the the args taken by the many factory methods of that class.  For
       other exceptions the first element of this array holds the class name of the exception */
    private Object[] args;

    public ErrorTransport() {
    }

    private ErrorTransport(String messageId,
                           boolean isStandardException,
                           boolean shouldRetry, Object[] args) {
        this.shouldRetry = shouldRetry;
        this.messageId = messageId;
        this.args = args;
        this.isStandard = isStandardException;
    }

    /**
     * Create a transport from a StandardException.
     */
    public static ErrorTransport newTransport(StandardException se) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "New StandardException  %s", se);
        }
        String mId = se.getMessageId();
        Object[] args = se.getArguments();
        return new ErrorTransport(mId, true, false, args);
    }

    /**
     * Create a transport from an arbitrary Throwable.
     */
    public static ErrorTransport newTransport(Throwable t) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "New Throwable  %s", t.getClass());
        }
        if(t instanceof ConstraintViolation.ConstraintViolationException) {
            // Have to convert ConstraintViolationException to StandardException before sending over the wire, otherwise
            // will lose information critical for end-user error message.
            StandardException se = Exceptions.toStandardException((ConstraintViolation.ConstraintViolationException) t);
            return newTransport(se);
        }
        t = Exceptions.getRootCause(t);
        if (t instanceof StandardException) {
            return newTransport((StandardException) t);
        }

        if(t instanceof WriteFailedException)
            return new ErrorTransport(t.getMessage(),false,true,new Object[]{WriteFailedException.class.getName(),((WriteFailedException)t).getTableName(),((WriteFailedException)t).getAttemptCount()});

        boolean shouldRetry = Exceptions.shouldRetry(t);
        if (!shouldRetry) {
            if (t instanceof DoNotRetryIOException) {
                String message = t.getMessage();
                if (message != null && message.contains("transaction") && message.contains("is not ACTIVE. State is ERROR")) {
                    shouldRetry = true;
                }
            }
        }
        String message = t.getMessage();

        return new ErrorTransport(message, false, shouldRetry, new Object[]{t.getClass().getName()});
    }

    /**
     * Get standard exception. Call this method only if you know the transport holds/represents a StandardException.
     */
    public StandardException getStandardException() {
        return StandardException.newException(messageId, args);
    }

    /**
     * Get the original Throwable.
     */
    public Throwable getError() {
        if (isStandard) {
            return getStandardException();
        }

        try {
            String className = args[0].toString();
            // Async Hbase Error Handling
            Class<? extends Throwable> errorClazz = (Class<? extends Throwable>) Class.forName(className);
            if(args.length>1){
                Object[] newConstructorArgs = new Object[args.length-1];
                System.arraycopy(args,1,newConstructorArgs,0,newConstructorArgs.length);
                Class[] paramTypes = new Class[newConstructorArgs.length];
                for(int i=0;i<newConstructorArgs.length;i++){
                    paramTypes[i] = newConstructorArgs[i].getClass();
                }
                Constructor constructor = ConstructorUtils.getAccessibleConstructor(errorClazz,paramTypes);
                return (Throwable)constructor.newInstance(newConstructorArgs);
            }

            /* one-arg string constructor */
            Constructor constructor = ConstructorUtils.getAccessibleConstructor(errorClazz, String.class);
            if (constructor != null) {
                String constructorMayNotAcceptNullArg = messageId == null ? "null" : messageId;
                return (Throwable) ConstructorUtils.invokeConstructor(errorClazz, constructorMayNotAcceptNullArg);
            }

            /* no-arg constructor */
            return errorClazz.newInstance();
        } catch (ClassNotFoundException | InvocationTargetException | NoSuchMethodException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean shouldRetry() {
        return shouldRetry;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "writing external error message %s", messageId);
        }
        out.writeObject(messageId);
        out.writeBoolean(isStandard);
        out.writeBoolean(shouldRetry);
        out.writeInt(args != null ? args.length : 0);
        if (args != null) {
            for (Object o : args) {
                if (LOG.isTraceEnabled())
                    SpliceLogUtils.trace(LOG, "writing external error message %s", o.getClass().toString());
                out.writeBoolean(o != null);
                if (o != null)
                    out.writeObject(o);
            }
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        messageId = (String) in.readObject();
        isStandard = in.readBoolean();
        shouldRetry = in.readBoolean();
        args = new Object[in.readInt()];
        for (int i = 0; i < args.length; i++) {
            args[i] = in.readBoolean() ? in.readObject() : null;
        }
    }

}
