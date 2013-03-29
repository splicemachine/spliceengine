package com.splicemachine.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.RemoteExceptionHandler;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.ServerCallable;
import org.apache.hadoop.hbase.client.coprocessor.Exec;
import org.apache.hadoop.hbase.client.coprocessor.ExecResult;
import org.apache.hadoop.hbase.ipc.CoprocessorProtocol;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * ExecRPCInvoker which does not automatically retry failed invocations.
 *
 * @author Scott Fines
 * Created on: 3/20/13
 */
public class NoRetryExecRPCInvoker implements InvocationHandler {
    // LOG is NOT in hbase subpackage intentionally so that the default HBase
    // DEBUG log level does NOT emit RPC-level logging.
    private static final Logger LOG = Logger.getLogger(NoRetryExecRPCInvoker.class);

    private Configuration conf;
    private final HConnection connection;
    private Class<? extends CoprocessorProtocol> protocol;
    private final byte[] table;
    private final byte[] row;
    private final boolean refreshConnectionCache;
    private byte[] regionName;

    public NoRetryExecRPCInvoker(Configuration conf,
                          HConnection connection,
                          Class<? extends CoprocessorProtocol> protocol,
                          byte[] table,
                          byte[] row,
                          boolean refreshConnectionCache) {
        this.conf = conf;
        this.connection = connection;
        this.protocol = protocol;
        this.table = table;
        this.row = row;
        this.refreshConnectionCache = refreshConnectionCache;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Call: " + method.getName() + ", " + (args != null ? args.length : 0));
        }

        if (row != null) {
            final Exec exec = new Exec(conf, row, protocol, method, args);
            ServerCallable<ExecResult> callable =
                    new ServerCallable<ExecResult>(connection, table, row) {
                        public ExecResult call() throws Exception {
                            return server.execCoprocessor(location.getRegionInfo().getRegionName(),
                                    exec);
                        }
                    };

            ExecResult result = executeWithoutRetries(callable);

            this.regionName = result.getRegionName();
            if(LOG.isTraceEnabled()){
                LOG.trace("Result is region="+ Bytes.toStringBinary(regionName) +
                        ", value="+result.getValue());
            }
            return result.getValue();
        }

        return null;
    }

/****************************************************************************************************************/
    /*private helper method*/
    private ExecResult executeWithoutRetries(ServerCallable<ExecResult> callable) throws IOException {
        /*
         * This block is taken from HConnectionImplementation.getRegionServerWithoutRetries. We can't
         * use that method directly, because it does not allow us to specify whether or not to invalidate
         * the connection's region cache if an error occurs. Thus, we have to manually do the operation.
         */
        try {
            callable.beforeCall();
            callable.connect(refreshConnectionCache);
            return callable.call();
        } catch (Throwable t) {
            Throwable t2 = translateException(t);
            if (t2 instanceof IOException) {
                throw (IOException)t2;
            } else {
                throw new RuntimeException(t2);
            }
        } finally {
            callable.afterCall();
        }
    }

    private Throwable translateException(Throwable t) throws IOException {
        /*
         * Convenience error interpreter taken from HConnectionImplementation because the method isn't
         * public. Probably should move it to a more centralized, more easily dealt with scenario, but
         * this way we replicate Connection behavior more intelligently.
         */
        if (t instanceof UndeclaredThrowableException) {
            t = t.getCause();
        }
        if (t instanceof RemoteException) {
            RemoteException re = (RemoteException)t;
            t = RemoteExceptionHandler.decodeRemoteException(re);
        }
        if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
        }
        return t;
    }
}
