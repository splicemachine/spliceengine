package com.splicemachine.derby.ddl;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 12/31/15
 */
public class DDLConfiguration{
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
