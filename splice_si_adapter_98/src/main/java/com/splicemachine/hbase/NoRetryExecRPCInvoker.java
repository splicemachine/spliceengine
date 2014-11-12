package com.splicemachine.hbase;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.UndeclaredThrowableException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.log4j.Logger;

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
    private Class<? extends CoprocessorService> service;
    private final byte[] table;
    private final byte[] row;
    private final boolean refreshConnectionCache;
    private byte[] regionName;

    public NoRetryExecRPCInvoker(Configuration conf,
                          HConnection connection,
                          Class<? extends CoprocessorService> service,
                          byte[] table,
                          byte[] row,
                          boolean refreshConnectionCache) {
        this.conf = conf;
        this.connection = connection;
        this.service = service;
        this.table = table;
        this.row = row;
        this.refreshConnectionCache = refreshConnectionCache;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        if (LOG.isTraceEnabled()) {
            LOG.trace("Call: " + method.getName() + ", " + (args != null ? args.length : 0));
        }

        // FIXME: jc - how to replace this functionality
//        if (row != null) {
//            final Exec exec = new Exec(conf, row, service, method, args);
//            RegionServerCallable<ExecResult> callable =
//                    new RegionServerCallable<ExecResult>(connection, table, row) {
//                        public ExecResult call() throws Exception {
//                            return server.execCoprocessor(location.getRegionInfo().getRegionName(),
//                                    exec);
//                        }
//                    };
//
//            ExecResult result = executeWithoutRetries(callable);
//
//            this.regionName = result.getRegionName();
//            if(LOG.isTraceEnabled()){
//                LOG.trace("Result is region="+ Bytes.toStringBinary(regionName) +
//                        ", value="+result.getValue());
//            }
//            return result.getValue();
//        }
        return null;
    }

/****************************************************************************************************************/
    /*private helper method*/
// FIXME: jc - how to replace this functionality
//    private ExecResult executeWithoutRetries(RegionServerCallable<ExecResult> callable) throws IOException {
//        /*
//         * This block is taken from HConnectionImplementation.getRegionServerWithoutRetries. We can't
//         * use that method directly, because it does not allow us to specify whether or not to invalidate
//         * the connection's region cache if an error occurs. Thus, we have to manually do the operation.
//         */
//        try {
//            callable.beforeCall();
//            callable.connect(refreshConnectionCache);
//            return callable.call();
//        } catch (Throwable t) {
//            Throwable t2 = translateException(t);
//            if (t2 instanceof IOException) {
//                throw (IOException)t2;
//            } else {
//                throw new RuntimeException(t2);
//            }
//        } finally {
//            callable.afterCall();
//        }
//    }
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
            t = re.unwrapRemoteException();
        }
        if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
        }
        return t;
    }

    public byte[] getRegionName() {
        return regionName;
    }
}
