package com.splicemachine.si.api;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.primitives.Bytes;

/**
 * Repository for holding configuration keys for SI.
 * <p/>
 * Each specific architecture configuration should provide
 * a default value for each of these keys.
 *
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class SIConfigurations{
    public static final String CONGLOMERATE_TABLE_NAME = "SPLICE_CONGLOMERATE";
    public static final byte[] CONGLOMERATE_TABLE_NAME_BYTES = Bytes.toBytes(CONGLOMERATE_TABLE_NAME);

    public static final String completedTxnCacheSize="splice.txn.completedTxns.cacheSize";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE=1<<17; // want to hold lots of completed transactions

    public static final String completedTxnConcurrency="splice.txn.completedTxns.concurrency";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY=64;

    public static final String TRANSACTION_KEEP_ALIVE_INTERVAL="splice.txn.keepAliveIntervalMs";
    public static final long DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL=15000l;

    public static final String TRANSACTION_TIMEOUT="splice.txn.timeout";
    public static final long DEFAULT_TRANSACTION_TIMEOUT=10*DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL; //100 minutes

    public static final String TRANSACTION_KEEP_ALIVE_THREADS="splice.txn.keepAliveThreads";
    public static final int DEFAULT_KEEP_ALIVE_THREADS=4;

    public static final String READ_RESOLVER_THREADS = "splice.txn.readresolver.threads";
    private static final int DEFAULT_READ_RESOLVER_THREADS = 4;

    public static final String READ_RESOLVER_QUEUE_SIZE = "splice.txn.readresolver.queueSize";
    private static final int DEFAULT_READ_RESOLVER_QUEUE_SIZE=1<<16;

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
    public static final String TRANSACTION_LOCK_STRIPES ="splice.txn.concurrencyLevel";

    /**
     * The number of milliseconds the timestamp client should wait for the response.
     * Defaults to 60000 (60 seconds)
     */
    public static final String TIMESTAMP_CLIENT_WAIT_TIME = "splice.timestamp_server.clientWaitTime";
    private static final int DEFAULT_TIMESTAMP_CLIENT_WAIT_TIME = 60000;

    /**
     * The Port to bind the Timestamp Server connection to
     * Defaults to 60012
     */
    public static final String TIMESTAMP_SERVER_BIND_PORT = "splice.timestamp_server.port";
    private static final int DEFAULT_TIMESTAMP_SERVER_BIND_PORT = 60012;

    public static final String ACTIVE_TRANSACTION_CACHE_SIZE="splice.txn.activeCacheSize";
    private static final int DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE = 1<<12;

    public static final SConfiguration.Defaults defaults=new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
                case TRANSACTION_TIMEOUT: return DEFAULT_TRANSACTION_TIMEOUT;
                case TRANSACTION_KEEP_ALIVE_INTERVAL: return DEFAULT_TRANSACTION_KEEP_ALIVE_INTERVAL;
                default:
                    throw new IllegalArgumentException("No long default for key '"+key+"'");
            }
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                case completedTxnConcurrency: return DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY;
                case completedTxnCacheSize: return DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE;
                case TRANSACTION_KEEP_ALIVE_THREADS: return DEFAULT_KEEP_ALIVE_THREADS;
                case READ_RESOLVER_THREADS: return DEFAULT_READ_RESOLVER_THREADS;
                case READ_RESOLVER_QUEUE_SIZE: return DEFAULT_READ_RESOLVER_QUEUE_SIZE;
                case TIMESTAMP_CLIENT_WAIT_TIME: return DEFAULT_TIMESTAMP_CLIENT_WAIT_TIME;
                case TIMESTAMP_SERVER_BIND_PORT: return DEFAULT_TIMESTAMP_SERVER_BIND_PORT;
                case ACTIVE_TRANSACTION_CACHE_SIZE: return DEFAULT_ACTIVE_TRANSACTION_CACHE_SIZE;
                default:
                    throw new IllegalArgumentException("No SI default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case TRANSACTION_TIMEOUT:
                case TRANSACTION_KEEP_ALIVE_INTERVAL:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case completedTxnConcurrency:
                case completedTxnCacheSize:
                case TRANSACTION_KEEP_ALIVE_THREADS:
                case READ_RESOLVER_THREADS:
                case READ_RESOLVER_QUEUE_SIZE:
                case TRANSACTION_LOCK_STRIPES:
                case TIMESTAMP_CLIENT_WAIT_TIME:
                case TIMESTAMP_SERVER_BIND_PORT:
                case ACTIVE_TRANSACTION_CACHE_SIZE:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public boolean hasStringDefault(String key){
            return false;
        }

        @Override
        public String defaultStringFor(String key){
            throw new IllegalArgumentException("No SI default for key '"+key+"'");
        }

        @Override
        public boolean defaultBooleanFor(String key){
            throw new IllegalArgumentException("No SI default for key '"+key+"'");
        }

        @Override
        public boolean hasBooleanDefault(String key){
            return false;
        }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalArgumentException("No SI default for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };
}
