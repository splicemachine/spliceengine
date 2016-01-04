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
    private static final long DEFAULT_MAX_DDL_WAIT=TimeUnit.SECONDS.toMillis(240);

    public static final String DDL_REFRESH_INTERVAL = "splice.ddl.refreshIntervalSeconds";
    private static final long DEFAULT_DDL_REFRESH_INTERVAL=TimeUnit.SECONDS.toMillis(10);

    public static final SConfiguration.Defaults defaults=new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
                case MAX_DDL_WAIT: return DEFAULT_MAX_DDL_WAIT;
                case DDL_REFRESH_INTERVAL: return DEFAULT_DDL_REFRESH_INTERVAL;
                default:
                    throw new IllegalArgumentException("No long default for key '"+key+"'");
            }
        }

        @Override
        public int defaultIntFor(String key){
            switch(key){
                default:
                    throw new IllegalArgumentException("No SI default for key '"+key+"'");
            }
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case MAX_DDL_WAIT:
                case DDL_REFRESH_INTERVAL:
                    return true;
                default:
                    return false;
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                default:
                    return false;
            }
        }
    };
}
