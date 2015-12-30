package com.splicemachine.constants;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * Defines the schema used by SI for the transaction table and for additional metadata on data tables.
 */

@SuppressFBWarnings(value = "MS_CANNOT_BE_FINAL",justification = "Intentional")
public class SIConstants extends SpliceConstants {
    static {
        setParameters();
    }





    /*
     * The time, in ms, to wait between attempts at resolving a transaction which
     * is in the COMMITTING state (e.g. the amount of time to wait for a transaction
     * to move from COMMITTING to COMMITTED).
     */
    @SpliceConstants.Parameter public static final String COMMITTING_PAUSE="splice.txn.committing.pauseTimeMs";
    @SpliceConstants.DefaultValue(COMMITTING_PAUSE) public static final int DEFAULT_COMMITTING_PAUSE=1000;
    public static int committingPause;


    /*
     * We use lock-striping to manage concurrent modifications/reads to the Transaction table. That is,
     * each Transaction is grouped into a bucket, and in order to read or modify that transaction, you must
     * first acquire the lock for that bucket.
     *
     * As a general rule, the more stripes you have, the more parallelism you can sustain. However, there are
     * two limiting factors to this. The first is memory--each stripe requires a separate set of objects which
     * occupy heap space. The second is threading performance.
     *
     * By experimentation, I've determined that (on the Oracle 6 JVM, at least) a ReadWriteLock occupies ~250 bytes,
     * so the total memory occupied is ~250*STRIPES, where STRIPES is the number of stripes that we have. Thus,
     * we have a table of memory usage as follows:
     *
     * 16       --  ~4K
     * 32       --  ~8K
     * 64       --  ~16K
     * 128      --  ~32K
     * 256      --  ~64K
     * 512      --  ~125K
     * 1024     --  ~250K
     * 4096     --  ~1M
     * 8192     --  ~2M
     * 16384    --  ~4M
     * 32768    --  ~8M
     *
     * This is the size for each transaction region, so there are actually 16 times that number of stripes (
     * and thus 16 times the memory usage).
     *
     * This inclines us to choose fewer stripes. However, we want to sustain a high degree of concurrency,
     * so we want to choose the correct number of stripes. Thankfully, we have a total limiter.
     *
     * All access to the transaction table occurs remotely (through the HBase client API), which means
     * that our maximum concurrency is actually the number of concurrent network actions that can be made
     * to a single server--in other words, the IPC threads. Any concurrency level which is higher than that
     * will be useless concurrency, as only a maximum of that many threads will be used. Thus, a reasonable
     * default is the number of ipc threads configured for this system.
     *
     * Note that the Stripe count is always a power of 2(if you set it to a non-power of 2, then the striper
     * will choose the smallest power of 2 greater than what you set), so we will always have a concurrency level
     * which is >= the number of ipc threads, which should allow plenty of concurrency for our applications.
     *
     * However, if we see bottlenecks due to this lock striping, then we may increase it manually, given the
     * tradeoffs that we discuss in this note.
     *
     */
    @Parameter public static final String TRANSACTION_LOCK_STRIPES ="splice.txn.concurrencyLevel";
    public static int transactionlockStripes;


    @Parameter public static final String ACTIVE_TRANSACTION_CACHE_SIZE = "splice.txn.activeTxns.cacheSize";
    @DefaultValue(ACTIVE_TRANSACTION_CACHE_SIZE) public static final int DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE = 124;
    public static int activeTransactionCacheSize;

    @Parameter public static final String COMPLETED_TRANSACTION_CACHE_SIZE = "splice.txn.completedTxns.cacheSize";
    @DefaultValue(COMPLETED_TRANSACTION_CACHE_SIZE) public static final int DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE = 1<<17; // want to hold lots of completed transactions
    public static int completedTransactionCacheSize;

    @Parameter public static final String COMPLETED_TRANSACTION_CACHE_CONCURRENCY = "splice.txn.completedTxns.concurrency";
    @DefaultValue(COMPLETED_TRANSACTION_CACHE_CONCURRENCY) public static final int DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY = 64;
    public static int completedTransactionConcurrency;

    @Parameter public static final String ROLL_FORWARD_READ_RATE = "splice.txn.rollforward.readThroughput";
    @DefaultValue(ROLL_FORWARD_READ_RATE) public static final int DEFAULT_ROLL_FORWARD_READ_RATE=10000; //default is 10,000 rows/sec
    public static int rollForwardRate;

    @Parameter public static final String READ_RESOLVER_THREADS = "splice.txn.readresolver.threads";
    @DefaultValue(READ_RESOLVER_THREADS) public static final int DEFAULT_READ_RESOLVER_THREADS = 4;
    public static int readResolverThreads;

    @Parameter public static final String READ_RESOLVER_QUEUE_SIZE = "splice.txn.readresolver.queueSize";
    @DefaultValue(READ_RESOLVER_QUEUE_SIZE)public static final int DEFAULT_READ_RESOLVER_QUEUE_SIZE=1<<16;
    public static int readResolverQueueSize;

    public static void setParameters(Configuration config){
        committingPause = config.getInt(COMMITTING_PAUSE,DEFAULT_COMMITTING_PAUSE);
        /*
         * The transaction timeout is the length of time (in milliseconds) after which a transaction is considered
         * unresponsive, and should be removed.
         *
         * Generally speaking, we want to allow enough time to make sure that the server is actually dead without
         * allowing so much time that transactions are left hanging out (and then requiring that we kill
         * the transaction manually to perform DDL). This is a bit of a balancing act: thankfully, we have
         * other systems that we configure as well that we can piggy back on to get a good timeout number.
         *
         * The ZooKeeper session timeout marks the absolute limit at which we are able to remain alive--if
         * we go that time period without checking in with ZooKeeper, then the RegionServer is definitely dead. If
         * the regionserver is dead, then it can't keep transactions alive, and so the transaction can safely time out.
         * Of course, we want to make sure that the zookeeper timeout definitely happened, so we add some slop
         * to the timeout window just to be safe.
         *
         */
        int zkTimeout = config.getInt(HConstants.ZK_SESSION_TIMEOUT,HConstants.DEFAULT_ZK_SESSION_TIMEOUT);
        int configuredTxnTimeout = config.getInt(TRANSACTION_TIMEOUT,DEFAULT_TRANSACTION_TIMEOUT);
        if(configuredTxnTimeout<zkTimeout)
            configuredTxnTimeout = (int)(1.5f*zkTimeout); //add some slop factor so that we are sure that zookeeper timed out first

        int ipcThreads = SpliceConstants.ipcThreads;
        transactionlockStripes = config.getInt(TRANSACTION_LOCK_STRIPES,ipcThreads);
        activeTransactionCacheSize = config.getInt(ACTIVE_TRANSACTION_CACHE_SIZE,DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE);
        completedTransactionCacheSize = config.getInt(COMPLETED_TRANSACTION_CACHE_SIZE,DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE);
        completedTransactionConcurrency = config.getInt(COMPLETED_TRANSACTION_CACHE_CONCURRENCY,DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY);
        rollForwardRate = config.getInt(ROLL_FORWARD_READ_RATE,DEFAULT_ROLL_FORWARD_READ_RATE);

        readResolverThreads = config.getInt(READ_RESOLVER_THREADS,DEFAULT_READ_RESOLVER_THREADS);
        readResolverQueueSize = config.getInt(READ_RESOLVER_QUEUE_SIZE,DEFAULT_READ_RESOLVER_QUEUE_SIZE);
    }
}
