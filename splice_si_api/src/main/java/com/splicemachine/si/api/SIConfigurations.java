package com.splicemachine.si.api;

/**
 * Repository for holding configuration keys for SI.
 *
 * Each specific architecture configuration should provide
 *  a default value for each of these keys.
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class SIConfigurations{


   public static final String completedTxnCacheSize= "splice.txn.completedTxns.cacheSize";
   public static final String completedTxnConcurrency = "splice.txn.completedTxns.concurrency";
}
