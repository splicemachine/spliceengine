package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLConfiguration{
    public static final String ERROR_TAG = "[ERROR]";

    public static final String MAX_DDL_WAIT = "splice.ddl.maxWaitSeconds";
    //-sf- tuned down to facilitate testing. Tune up if it's causing problems
    private static final long DEFAULT_MAX_DDL_WAIT=TimeUnit.SECONDS.toMillis(60);

    public static final String DDL_REFRESH_INTERVAL = "splice.ddl.refreshIntervalSeconds";
    private static final long DEFAULT_DDL_REFRESH_INTERVAL=TimeUnit.SECONDS.toMillis(10);

    /**
     * The initial wait in milliseconds when a DDL operation waits for all concurrent transactions to finish before
     * proceeding.
     *
     * The operation will wait progressively longer until the DDL_DRAINING_MAXIMUM_WAIT is reached, then it will
     * block concurrent transactions from writing to the affected tables.
     *
     * Defaults to 1000 (1 second)
     */
    public  static final String DDL_DRAINING_INITIAL_WAIT = "splice.ddl.drainingWait.initial";
    private static final long DEFAULT_DDL_DRAINING_INITIAL_WAIT = 1000;

    /**
     * The maximum wait in milliseconds a DDL operation will wait for concurrent transactions to finish before
     * blocking them from writing to the affected tables.
     *
     * Defaults to 100000 (100 seconds)
     */
    public static final String DDL_DRAINING_MAXIMUM_WAIT = "splice.ddl.drainingWait.maximum";
    private static final long DEFAULT_DDL_DRAINING_MAXIMUM_WAIT = 100000;

    public static final SConfiguration.Defaults defaults=new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
                case MAX_DDL_WAIT: return DEFAULT_MAX_DDL_WAIT;
                case DDL_REFRESH_INTERVAL: return DEFAULT_DDL_REFRESH_INTERVAL;
                case DDL_DRAINING_INITIAL_WAIT: return DEFAULT_DDL_DRAINING_INITIAL_WAIT;
                case DDL_DRAINING_MAXIMUM_WAIT: return DEFAULT_DDL_DRAINING_MAXIMUM_WAIT;
                default:
                    throw new IllegalArgumentException("No long default for key '"+key+"'");
            }
        }
        @Override
        public boolean hasIntDefault(String key){
            return false;
        }

        @Override
        public int defaultIntFor(String key){
            throw new IllegalArgumentException("No SI default for key '"+key+"'");
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case MAX_DDL_WAIT:
                case DDL_REFRESH_INTERVAL:
                case DDL_DRAINING_INITIAL_WAIT:
                case DDL_DRAINING_MAXIMUM_WAIT:
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
            throw new IllegalArgumentException("No DDL default for key '"+key+"'");
        }

        @Override
        public boolean defaultBooleanFor(String key){
            throw new IllegalArgumentException("No DDL default for key '"+key+"'");
        }

        @Override
        public boolean hasBooleanDefault(String key){
            return false;
        }

        @Override
        public double defaultDoubleFor(String key){
            throw new IllegalArgumentException("No DDL default for key '"+key+"'");
        }

        @Override
        public boolean hasDoubleDefault(String key){
            return false;
        }
    };
}
