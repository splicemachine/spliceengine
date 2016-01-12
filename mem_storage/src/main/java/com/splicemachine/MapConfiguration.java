package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.util.ChainedDefaults;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MapConfiguration implements SConfiguration{
    private final Map<String,String> configurations;
    private final ChainedDefaults defaults = new ChainedDefaults();

    public MapConfiguration(){
        this.configurations = new ConcurrentHashMap<>();
    }

    @Override
    public double getDouble(String key){
        String v = configurations.get(key);
        if(v==null)
            return defaults.defaultDoubleFor(key);
        return Double.parseDouble(v);
    }

    @Override
    public boolean getBoolean(String key){
        String v = configurations.get(key);
        boolean b;
        if(v==null){
           b = defaults.defaultBooleanFor(key);
        }else
            b = Boolean.parseBoolean(v);
        return b;
    }

    @Override
    public int getInt(String key){
        String v=configurations.get(key);
        if(v==null) return defaults.defaultIntFor(key);
        else return Integer.parseInt(v);
    }

    @Override
    public long getLong(String key){
        String v=configurations.get(key);
        if(v==null) return defaults.defaultLongFor(key);
        else return Long.parseLong(v);
    }

    @Override
    public String getString(String key){
        String value = configurations.get(key);
        if(value==null) value = defaults.defaultStringFor(key);
        return value;
    }

    @Override
    public Set<String> prefixMatch(String prefix){
        Set<String> strings = new HashSet<>();
        for(String key:configurations.keySet()){
            if(key.startsWith(prefix))strings.add(key);
        }
        //todo -sf- add in default values
        return strings;
    }



    @Override
    public void addDefaults(Defaults defaults){
        this.defaults.addDefaults(defaults);
    }

}
