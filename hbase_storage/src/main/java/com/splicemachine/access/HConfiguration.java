package com.splicemachine.access;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.si.api.SIConfigurations;
import org.apache.hadoop.conf.Configuration;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HConfiguration implements SConfiguration{
    private final Configuration delegate;

    public HConfiguration(Configuration delegate){
        this.delegate=delegate;
    }

    @Override
    public long getLong(String key){
        return delegate.getLong(key,SIConfigurations.defaultLongFor(key));
    }

    @Override
    public int getInt(String key){
        return delegate.getInt(key,SIConfigurations.defaultIntFor(key));
    }
}
