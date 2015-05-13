package org.apache.hadoop.hbase.ipc;

import com.google.protobuf.ServiceException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetryingCallable;
import org.apache.hadoop.hbase.client.RpcRetryingCaller;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.util.ExceptionUtil;
import org.apache.hadoop.hbase.util.ReflectionUtils;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.lang.reflect.UndeclaredThrowableException;
import java.net.SocketTimeoutException;
import java.util.LinkedList;
import java.util.List;

/**
 * Splice version of RpcRetryingCallerFactory.
 *
 * The reason this exists is HBASE-11374(https://issues.apache.org/jira/browse/HBASE-11374)
 * which makes it so that setting the socket timeout is ignored when we use the noretry rpc invocations(
 * i.e. the bulk write path). Since this socket timeout may be tuned for number of reasons, it
 * is worth it to put fixes in place for it.
 *
 * @author Scott Fines
 * Date: 10/6/14
 */
public class SpliceRetryingCallerFactory  {

    /** Configuration key for a custom {@link org.apache.hadoop.hbase.client.RpcRetryingCaller} */
    public static final String CUSTOM_CALLER_CONF_KEY = "hbase.rpc.callerfactory.class";
    protected final Configuration conf;
    private final long pause;
    private final int retries;
    private final int socketTimeout;

    public SpliceRetryingCallerFactory(Configuration conf) {
        this.conf = conf;

        pause = conf.getLong(HConstants.HBASE_CLIENT_PAUSE,
                HConstants.DEFAULT_HBASE_CLIENT_PAUSE);
        retries = conf.getInt(HConstants.HBASE_CLIENT_RETRIES_NUMBER,
                HConstants.DEFAULT_HBASE_CLIENT_RETRIES_NUMBER);
        socketTimeout = getSocketTimeout(conf);
    }

    public <T> SpliceRpcRetryingCaller<T> newCaller() {
        // We store the values in the factory instance. This way, constructing new objects
        //  is cheap as it does not require parsing a complex structure.
        return new SpliceRpcRetryingCaller<T>(pause, retries,socketTimeout);
    }

    public static SpliceRetryingCallerFactory instantiate(Configuration configuration) {
        String rpcCallerFactoryClazz =
                configuration.get(RpcRetryingCallerFactory.CUSTOM_CALLER_CONF_KEY,
                        SpliceRetryingCallerFactory.class.getName());
        return ReflectionUtils.instantiateWithCustomCtor(rpcCallerFactoryClazz,
                new Class[]{Configuration.class}, new Object[]{configuration});
    }

    public static class SpliceRpcRetryingCaller<T>  {
        static final Log LOG = LogFactory.getLog(RpcRetryingCaller.class);
        private final int socketTimeout;
        /**
         * Timeout for the call including retries
         */
        private int callTimeout;
        /**
         * When we started making calls.
         */
        private long globalStartTime;
        /**
         * Start and end times for a single call.
         */
        private final static int MIN_RPC_TIMEOUT = 2000;

        private final long pause;
        private final int retries;

        private int remaining;

        public SpliceRpcRetryingCaller(long pause, int retries,int socketTimeout) {
            this.pause = pause;
            this.retries = retries;
            this.socketTimeout = socketTimeout;
        }

        private void beforeCall() {
            int remaining = (int)(callTimeout - (System.currentTimeMillis() - this.globalStartTime));
            if (remaining < socketTimeout) {
                // If there is no time left, we're trying anyway. It's too late.
                // 0 means no timeout, and it's not the intent here. So we secure both cases by
                // resetting to the minimum.
                remaining = socketTimeout;
            }
            this.remaining = remaining;
        }


        public synchronized T callWithRetries(SpliceRetryingCall<T> callable) throws IOException,
                RuntimeException {
            return callWithRetries(callable, HConstants.DEFAULT_HBASE_CLIENT_OPERATION_TIMEOUT);
        }

        /**
         * Retries if invocation fails.
         * @param callTimeout Timeout for this call
         * @param callable The {@link RetryingCallable} to run.
         * @return an object of type T
         * @throws IOException if a remote or network exception occurs
         * @throws RuntimeException other unspecified error
         */
        @edu.umd.cs.findbugs.annotations.SuppressWarnings
                (value = "SWL_SLEEP_WITH_LOCK_HELD", justification = "na")
        public synchronized T callWithRetries(SpliceRetryingCall<T> callable, int callTimeout)
                throws IOException, RuntimeException {
            this.callTimeout = callTimeout;
            List<RetriesExhaustedException.ThrowableWithExtraContext> exceptions = new LinkedList<>();
            this.globalStartTime = System.currentTimeMillis();
            for (int tries = 0;; tries++) {
                long expectedSleep = 0;
                try {
                    beforeCall();
                    callable.prepare(tries != 0); // if called with false, check table status on ZK
                    return callable.call(remaining);
                } catch (Throwable t) {
                    if (LOG.isTraceEnabled()) {
                        LOG.trace("Call exception, tries=" + tries + ", retries=" + retries + ", retryTime=" +
                                (System.currentTimeMillis() - this.globalStartTime) + "ms", t);
                    }
                    // translateException throws exception when should not retry: i.e. when request is bad.
                    t = translateException(t);
                    callable.throwable(t, retries != 1);
                    RetriesExhaustedException.ThrowableWithExtraContext qt =
                            new RetriesExhaustedException.ThrowableWithExtraContext(t,
                                    System.currentTimeMillis(), toString());
                    exceptions.add(qt);
                    ExceptionUtil.rethrowIfInterrupt(t);
                    if (tries >= retries - 1) {
                        throw new RetriesExhaustedException(tries, exceptions);
                    }
                    // If the server is dead, we need to wait a little before retrying, to give
                    //  a chance to the regions to be
                    // tries hasn't been bumped up yet so we use "tries + 1" to get right pause time
                    expectedSleep = callable.sleep(pause, tries + 1);

                    // If, after the planned sleep, there won't be enough time left, we stop now.
                    long duration = singleCallDuration(expectedSleep);
                    if (duration > this.callTimeout) {
                        String msg = "callTimeout=" + this.callTimeout + ", callDuration=" + duration +
                                ": " + callable.getExceptionMessageAdditionalDetail();
                        throw (SocketTimeoutException)(new SocketTimeoutException(msg).initCause(t));
                    }
                } finally {
                    callable.close();
                }
                try {
                    Thread.sleep(expectedSleep);
                } catch (InterruptedException e) {
                    throw new InterruptedIOException("Interrupted after " + tries + " tries  on " + retries);
                }
            }
        }

        /**
         * @param expectedSleep
         * @return Calculate how long a single call took
         */
        private long singleCallDuration(final long expectedSleep) {
            return (System.currentTimeMillis() - this.globalStartTime)
                    + MIN_RPC_TIMEOUT + expectedSleep;
        }

        /**
         * Call the server once only.
         * {@link RetryingCallable} has a strange shape so we can do retrys.  Use this invocation if you
         * want to do a single call only (A call to {@link SpliceRetryingCall#call(int)} will not likely
         * succeed).
         * @return an object of type T
         * @throws IOException if a remote or network exception occurs
         * @throws RuntimeException other unspecified error
         */
        public T callWithoutRetries(SpliceRetryingCall<T> callable) throws IOException, RuntimeException {
            // The code of this method should be shared with withRetries.
            this.globalStartTime = System.currentTimeMillis();
            try {
                beforeCall();
                callable.prepare(false);
                return callable.call(remaining);
            } catch (Throwable t) {
                Throwable t2 = translateException(t);
                ExceptionUtil.rethrowIfInterrupt(t2);
                // It would be nice to clear the location cache here.
                if (t2 instanceof IOException) {
                    throw (IOException)t2;
                } else {
                    throw new RuntimeException(t2);
                }
            } finally {
                callable.close();
            }
        }

        /**
         * Get the good or the remote exception if any, throws the DoNotRetryIOException.
         * @param t the throwable to analyze
         * @return the translated exception, if it's not a DoNotRetryIOException
         * @throws org.apache.hadoop.hbase.DoNotRetryIOException - if we find it, we throw it instead of translating.
         */
        static Throwable translateException(Throwable t) throws DoNotRetryIOException {
            if (t instanceof UndeclaredThrowableException) {
                if (t.getCause() != null) {
                    t = t.getCause();
                }
            }
            if (t instanceof RemoteException) {
                t = ((RemoteException)t).unwrapRemoteException();
            }
            if (t instanceof LinkageError) {
                throw new DoNotRetryIOException(t);
            }
            if (t instanceof ServiceException) {
                ServiceException se = (ServiceException)t;
                Throwable cause = se.getCause();
                if (cause != null && cause instanceof DoNotRetryIOException) {
                    throw (DoNotRetryIOException)cause;
                }
                // Don't let ServiceException out; its rpc specific.
                t = cause;
                // t could be a RemoteException so go aaround again.
                translateException(t);
            } else if (t instanceof DoNotRetryIOException) {
                throw (DoNotRetryIOException)t;
            }
            return t;
        }
    }

    /*Put here to avoid platform-specific issues*/
    private final static String H100SOCKET_TIMEOUT = "hbase.ipc.client.socket.timeout.read";
    private final static String SOCKET_TIMEOUT = "ipc.socket.timeout";
    private final static int DEFAULT_SOCKET_TIMEOUT = 20000; // 20 seconds
    private static int getSocketTimeout(Configuration conf) {
        /*
         * Hbase 1.0.0 switches the configuration to using the socket.timeout.read version instead
         * of ipc.socket.timeout. We need to support both (for the 0.98+ versions), so we will
         * first try and get the 1.0.0 version, and if that fails, we will fall back on the 0.98+ version
         */
        String readTimeout = conf.get(H100SOCKET_TIMEOUT);
        if(readTimeout!=null && readTimeout.length()>0)
            return Integer.parseInt(readTimeout);
        else
            return conf.getInt(SOCKET_TIMEOUT,DEFAULT_SOCKET_TIMEOUT);
    }
}