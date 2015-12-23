package com.splicemachine.access;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.util.ChainedDefaults;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HConfiguration implements SConfiguration{
    private final Configuration delegate;
    private final ChainedDefaults defaults;

    public HConfiguration(Configuration delegate,SConfiguration.Defaults defaults){
        this.delegate=delegate;
        if(defaults instanceof ChainedDefaults){
            this.defaults = (ChainedDefaults)defaults;
        }else{
            this.defaults = new ChainedDefaults();
            if(defaults!=null)
                this.defaults.addDefaults(defaults);
        }
        this.defaults.addDefaults(HBASE_DEFAULTS);
    }

    @Override
    public long getLong(String key){
        return delegate.getLong(key,defaults.defaultLongFor(key));
    }

    @Override
    public int getInt(String key){
        return delegate.getInt(key,defaults.defaultIntFor(key));
    }

    public void addDefaults(SConfiguration.Defaults defaults){
        this.defaults.addDefaults(defaults);
    }

    private static final Defaults HBASE_DEFAULTS= new Defaults(){
        @Override
        public boolean hasLongDefault(String key){
            return false;
        }

        @Override
        public long defaultLongFor(String key){
            throw new IllegalArgumentException("No hbase default for key '"+key+"'");
        }

        @Override
        public boolean hasIntDefault(String key){
            switch(key){
                case HConstants.REGION_SERVER_HANDLER_COUNT: return true;
                default:return false;
            }
        }

        @Override
        public int defaultIntFor(String key){
            assert hasIntDefault(key): "No hbase default for key '"+key+"'";
            switch(key){
                case HConstants.REGION_SERVER_HANDLER_COUNT: return HConstants.DEFAULT_REGION_SERVER_HANDLER_COUNT;
                default:
                    throw new IllegalArgumentException("No Hbase default for key '"+key+"'");
            }
        }
    };
}
