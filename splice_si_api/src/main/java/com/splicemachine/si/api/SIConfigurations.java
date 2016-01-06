package com.splicemachine.si.api;

import com.splicemachine.access.api.SConfiguration;

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
    };
}
