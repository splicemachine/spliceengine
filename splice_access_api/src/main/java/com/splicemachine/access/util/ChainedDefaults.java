package com.splicemachine.access.util;

import com.splicemachine.access.api.SConfiguration;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * @author Scott Fines
 *         Date: 12/28/15
 */
public class ChainedDefaults implements SConfiguration.Defaults{
    private List<SConfiguration.Defaults> defaults = new CopyOnWriteArrayList<>();

    public void addDefaults(SConfiguration.Defaults defaults){
        this.defaults.add(defaults);
    }

    @Override
    public boolean hasLongDefault(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasLongDefault(key)) return true;
        }
        return false;
    }

    @Override
    public long defaultLongFor(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasLongDefault(key))
                return def.defaultLongFor(key);
        }
        throw new IllegalStateException("No default long set for key '"+ key+"'");
    }

    @Override
    public boolean hasIntDefault(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasIntDefault(key)) return true;
        }
        return false;
    }

    @Override
    public int defaultIntFor(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasIntDefault(key))
                return def.defaultIntFor(key);
        }
        throw new IllegalStateException("No default int set for key '"+ key+"'");
    }

    @Override
    public boolean hasStringDefault(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasStringDefault(key)) return true;
        }
        return false;
    }

    @Override
    public String defaultStringFor(String key){
        for(SConfiguration.Defaults def:defaults){
            if(def.hasStringDefault(key))
                return def.defaultStringFor(key);
        }
        throw new IllegalStateException("No default string set for key '"+key+"'");
    }
}
