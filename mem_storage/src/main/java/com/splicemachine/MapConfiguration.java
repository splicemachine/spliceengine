package com.splicemachine;

import com.splicemachine.access.api.SConfiguration;

import java.util.Map;
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

    public void setDefault(String key, long value){
        this.defaults.put(key,Long.toString(value));
    }
}
