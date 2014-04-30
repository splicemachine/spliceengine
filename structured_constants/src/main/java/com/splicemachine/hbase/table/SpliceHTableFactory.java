package com.splicemachine.hbase.table;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTableInterfaceFactory;
import org.apache.hadoop.hbase.client.SpliceHConnection;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;

public class SpliceHTableFactory implements HTableInterfaceFactory {
    private static Logger LOG = Logger.getLogger(SpliceHTableFactory.class);
    private static final int DEFAULT_NUM_CONNECTIONS = 10;

    private boolean autoFlush = true;
    private ExecutorService tablePool;
    private IntObjectOpenHashMap<HConnection> connections = new IntObjectOpenHashMap<HConnection>();
    private static AtomicInteger increment = new AtomicInteger();
    private final int numConnections;

    public SpliceHTableFactory() {
        this(true);
    }

    public SpliceHTableFactory(boolean autoFlush) {
			this(autoFlush,DEFAULT_NUM_CONNECTIONS);
		}

    public SpliceHTableFactory(boolean autoFlush,int numConnections) {
        SpliceLogUtils.trace(LOG, "instantiated with autoFlush set to %s", autoFlush);
        this.autoFlush = autoFlush;
        this.numConnections = numConnections;
        Configuration config = SpliceConstants.config;
        tablePool = createHTablePool(config);
        ExecutorService connectionPool = createConnectionPool(config);

        for (int i = 0; i< numConnections; i++) {
            Configuration configuration = new Configuration(SpliceConstants.config);
            configuration.setInt(HConstants.HBASE_CLIENT_INSTANCE_ID,i);
            try {
                connections.put(i, new SpliceHConnection(config, connectionPool));
            } catch (ZooKeeperConnectionException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public HTableInterface createHTableInterface(Configuration config, final byte[] tableName) {
        SpliceLogUtils.trace(LOG, "createHTableInterface for %s", Bytes.toString(tableName));
        try {
            SpliceLogUtils.trace(LOG, "creating actual HTable after connection created");
            final HTable htable = new SpliceHTable(tableName, connections.get(increment.incrementAndGet()%numConnections),
                    tablePool, HBaseRegionCache.getInstance());
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

    private static ExecutorService createHTablePool(Configuration config) {
        int maxThreads = config.getInt("hbase.htable.threads.max", Integer.MAX_VALUE);
        if (maxThreads == 0)
            maxThreads = 1;

        long keepAliveTime = config.getLong("hbase.htable.threads.keepalivetime", 60);

        return new ThreadPoolExecutor(1, maxThreads, keepAliveTime, TimeUnit.SECONDS,
                new SynchronousQueue<Runnable>(),
                new NamedThreadFactory("htable-pool-"),
                new ThreadPoolExecutor.CallerRunsPolicy());
    }

    private static ExecutorService createConnectionPool(Configuration conf) {
        int coreThreads = conf.getInt("hbase.hconnection.threads.core", 10);
        if (coreThreads == 0) {
            coreThreads = 10;
        }
        int maxThreads = conf.getInt("hbase.hconnection.threads.max", 32);
        if (maxThreads == 0) {
            maxThreads = Runtime.getRuntime().availableProcessors() * 8;
        }
        long keepAliveTime = conf.getLong("hbase.hconnection.threads.keepalivetime", 60);
        LinkedBlockingQueue<Runnable> workQueue =
                new LinkedBlockingQueue<Runnable>(maxThreads *
                        conf.getInt(HConstants.HBASE_CLIENT_MAX_TOTAL_TASKS,
                                HConstants.DEFAULT_HBASE_CLIENT_MAX_TOTAL_TASKS));
        return new ThreadPoolExecutor(coreThreads,  maxThreads, keepAliveTime, TimeUnit.SECONDS,
                workQueue,
                new NamedThreadFactory("connection-pool-"),
                new ThreadPoolExecutor.AbortPolicy());

    }

    private static class NamedThreadFactory implements ThreadFactory {
        private ThreadGroup group;
        private String namePrefix;
        private AtomicInteger threadNumber = new AtomicInteger(1);

        private NamedThreadFactory(String namePrefix) {
            SecurityManager s = System.getSecurityManager();
            group = (s != null) ? s.getThreadGroup() : Thread.currentThread().getThreadGroup();
            this.namePrefix = namePrefix;
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