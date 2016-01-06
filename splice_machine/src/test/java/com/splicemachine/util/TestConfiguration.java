package com.splicemachine.util;

import com.splicemachine.access.api.SConfiguration;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class TestConfiguration implements SConfiguration{
    private final Map<String,String> configurations;
    private final Map<String,String> defaults;

    public TestConfiguration(){
        this.configurations = new HashMap<>();
        this.defaults = new HashMap<>();
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

    @Override
    public String getString(String key){
        String s=configurations.get(key);
        if(s==null)
            s = defaults.get(key);
        return s;
    }

    @Override
    public Set<String> prefixMatch(String prefix){
        Set<String> configs = new HashSet<>(configurations.size()+defaults.size(),0.9f);
        if(prefix==null||prefix.length()<=0){
            configs.addAll(configurations.keySet());
            configs.addAll(defaults.keySet());
        }else{
            for(String configKey : configurations.keySet()){
                if(configKey.startsWith(prefix))
                    configs.add(configKey);
            }
            for(String configKey : defaults.keySet()){
                if(configKey.startsWith(prefix))
                    configs.add(configKey);
            }
        }
        return configs;
    }

    @Override
    public double getDouble(String key){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean getBoolean(String key){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    public void setDefault(String key, long value){
        this.defaults.put(key,Long.toString(value));
    }
}
