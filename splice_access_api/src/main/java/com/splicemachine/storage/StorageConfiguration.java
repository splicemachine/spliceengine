package com.splicemachine.storage;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class StorageConfiguration{

    /**
     * Amount of time(in milliseconds) taken to wait for a Region split to occur before checking on that
     * split's status during internal Split operations. It is generally not recommended
     * to adjust this setting unless Region splits take an incredibly short or long amount
     * of time to complete.
     *
     * Defaults to 500 ms.
     */
    public static final String TABLE_SPLIT_SLEEP_INTERVAL= "splice.splitWaitInterval";
    public static final long DEFAULT_SPLIT_WAIT_INTERVAL = 500l;

    public static final String REGION_MAX_FILE_SIZE = "hbase.hregion.max.filesize";

    public static final SConfiguration.Defaults defaults = new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
                case TABLE_SPLIT_SLEEP_INTERVAL: return DEFAULT_SPLIT_WAIT_INTERVAL;
                default:
                    throw new IllegalArgumentException("No long default for key '"+key+"'");
            }
        }

        @Override
        public int defaultIntFor(String key){
            throw new IllegalArgumentException("No int default for key '"+key+"'");
        }

        @Override
        public boolean hasLongDefault(String key){
            switch(key){
                case TABLE_SPLIT_SLEEP_INTERVAL: return true;
                default:return false;
            }
        }

        @Override
        public boolean hasIntDefault(String key){
            return false;
        }

        @Override
        public boolean hasStringDefault(String key){
            return false;
        }

        @Override
        public String defaultStringFor(String key){
            throw new IllegalArgumentException("No String default for key '"+key+"'");
        }
    };
}
