package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;

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
    private final Map<String,String> defaults;

    public MapConfiguration(){
        this.configurations = new ConcurrentHashMap<>();
        this.defaults = new ConcurrentHashMap<>();
    }

    @Override
    public double getDouble(String key){
        String v = getString(key);
        if(v==null)
            throw new IllegalStateException("No default found for key '"+key+"'");
        return Double.parseDouble(v);
    }

    @Override
    public boolean getBoolean(String key){
        String v = getString(key);
        if(v==null)
            throw new IllegalStateException("No default found for key '"+key+"'");
        return Boolean.parseBoolean(v);
    }

    @Override
    public String getString(String key){
        String value = configurations.get(key);
        if(value==null) return defaults.get(key);
        return null;
    }

    @Override
    public Set<String> prefixMatch(String prefix){
        Set<String> strings = new HashSet<>();
        for(String key:configurations.keySet()){
            if(key.startsWith(prefix))strings.add(key);
        }
        for(String key:defaults.keySet()){
            if(key.startsWith(prefix))strings.add(key);
        }
        return strings;
    }

    @Override
    public long getLong(String key){
        String s=configurations.get(key);
        if(s==null)
            s = defaults.get(key);
        if(s==null) return 0l;
        return Long.parseLong(s);
    }

    @Override
    public int getInt(String key){
        String s=configurations.get(key);
        if(s==null)
            s = defaults.get(key);
        if(s==null) return 0;
        return Integer.parseInt(s);
    }

    @Override
    public void addDefaults(Defaults defaults){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public void setDefault(String key, long value){
        this.defaults.put(key,Long.toString(value));
    }
}
