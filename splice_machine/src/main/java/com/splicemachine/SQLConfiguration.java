package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class SQLConfiguration{
    public static final String SPLICE_DB = "splicedb";
    public static final String SPLICE_USER = "SPLICE";
    public static final String SPLICE_JDBC_DRIVER = "com.splicemachine.db.jdbc.ClientDriver";

    public static final SConfiguration.Defaults defaults=new SConfiguration.Defaults(){
        @Override
        public long defaultLongFor(String key){
            switch(key){
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
