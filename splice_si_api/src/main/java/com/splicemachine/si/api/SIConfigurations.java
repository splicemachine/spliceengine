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

    public static final String completedTxnCacheSize = "splice.txn.completedTxns.cacheSize";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE = 1<<17; // want to hold lots of completed transactions

    public static final String completedTxnConcurrency="splice.txn.completedTxns.concurrency";
    private static final int DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY = 64;

    public static final SConfiguration.Defaults defaults = new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            throw new IllegalArgumentException("No SI default for key '"+key+"'");
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                case completedTxnConcurrency: return DEFAULT_COMPLETED_TRANSACTION_CONCURRENCY;
                case completedTxnCacheSize: return DEFAULT_COMPLETED_TRANSACTION_CACHE_SIZE;
                default:
                    throw new IllegalArgumentException("No SI default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasLongDefault(String key){
            return false;
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case completedTxnConcurrency:
                case completedTxnCacheSize:
                    return true;
                default:
                    return false;
            }
        }
    };
}
