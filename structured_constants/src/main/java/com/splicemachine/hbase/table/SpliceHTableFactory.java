package com.splicemachine.hbase.table;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.log4j.Logger;

public class SpliceHTableFactory implements HTableInterfaceFactory {
    protected static AtomicInteger increment = new AtomicInteger();
    private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);
//    protected IntObjectOpenHashMap<HConnection> connections = new IntObjectOpenHashMap<HConnection>();
    private boolean autoFlush = true;
    private ThreadPoolExecutor tableExecutor;
    private Executor connectionPool;

    public SpliceHTableFactory() {
        this(true);
    }

    public SpliceHTableFactory(boolean autoFlush) {
        this(autoFlush, 10);
    }

    public SpliceHTableFactory(boolean autoFlush, int numConnections) {
        SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s", autoFlush);
        this.autoFlush = autoFlush;
        tableExecutor = createHTablePool(SpliceConstants.config);
        connectionPool = createConnectionPool(SpliceConstants.config);

        /*
         * TODO: JC - check that this is still necessary
         * This was put in place for TPCC behavior, but in 0.96, for some (currently
         * unknown reason) this causes hangs when you try to use these connections
         * instead of those pooled in HConnectionManager. At some point we'll need
         * to try it again, but for now, it's just ripped out.
         */
//        for (int i = 0; i < numConnections; i++) {
//            Configuration configuration = new Configuration(SpliceConstants.config);
//            configuration.setInt(HConstants.HBASE_CLIENT_INSTANCE_ID, i);
//            try {
//                connections.put(i, SpliceHConnection.createConnection(configuration));
//            } catch (ZooKeeperConnectionException e) {
//                throw new RuntimeException(e);
//            } catch (IOException e) {
//                throw new RuntimeException(e);
//            }
//        }
    }

    private ThreadPoolExecutor createHTablePool(Configuration config) {
        int maxThreads = config.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
        if (maxThreads == 0)
            maxThreads = 1;

        long keepAliveTime = config.getLong("hbase.htable.threads.keepalivetime", 60);

        return new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                                      new SynchronousQueue<Runnable>(),
                                      new NamedThreadFactory(),
                                      new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private Executor createConnectionPool(Configuration conf) {
        int maxThreads = conf.getInt("hbase.hconnection.threads.max", 256);
        int coreThreads = conf.getInt("hbase.hconnection.threads.core", 0);
        if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
        }
        long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 10);
        LinkedBlockingQueue<Runnable> workQueue =
                new LinkedBlockingQueue<Runnable>(maxThreads *
                        conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
        return new ThreadPoolExecutor(
                coreThreads,
                maxThreads,
                keepAliveTime,
                TimeUnit.SECONDS,
                workQueue,
                Threads.newDaemonThreadFactory(toString() + "-shared-"));

    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, final byte[] tableName) {
        SpliceLogUtils.trace(LOG, "createHTableInterface for %s", Bytes.toString(tableName));
        try {
            SpliceLogUtils.trace(LOG, "creating actual HTable after connection created");
//            final HTable htable = new SpliceHTable(tableName, connections.get(increment.incrementAndGet() % 10),
//            final HTable htable = new SpliceHTable(tableName, SpliceHConnection.createConnection(config),
            final HTable htable = new SpliceHTable(tableName, HConnectionManager.getConnection(config),
                                                   tableExecutor, HBaseRegionCache.getInstance());
            htable.setAutoFlushTo(autoFlush);
            SpliceLogUtils.trace(LOG, "Returning created table");
            return htable;
        } catch (IOException ioe) {
            ioe.printStackTrace();
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public void releaseHTableInterface(HTableInterface table) throws IOException {
        SpliceLogUtils.trace(LOG, "releaseHTableInterface for %s", Bytes.toString(table.getTableName()));
        table.close();
    }

    private class NamedThreadFactory implements ThreadFactory {
        private ThreadGroup group;
        private String namePrefix;
        private AtomicInteger threadNumber = new AtomicInteger(1);

        private NamedThreadFactory() {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            namePrefix = "htable-pool-";
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(group, r, namePrefix + threadNumber.getAndIncrement(), 0);
            if (!t.isDaemon())
                t.setDaemon(true);
            if (t.getPriority() != Thread.NORM_PRIORITY)
                t.setPriority(Thread.NORM_PRIORITY);

            return t;
        }
    }
}