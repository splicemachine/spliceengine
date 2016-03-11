package com.splicemachine.si;
import java.util.Map;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.splicemachine.access.configuration.ConfigurationSource;

/**
 * Mocked up ConfigurationSource for testing. You get what you give it.
 */
public class ReflectingConfigurationSource implements ConfigurationSource {
    @Override
    public int getInt(String key, int deflt) {
        return deflt;
    }

    @Override
    public long getLong(String key, long deflt) {
        return deflt;
    }

    @Override
    public boolean getBoolean(String key, boolean deflt) {
        return deflt;
    }

    @Override
    public String getString(String key, String deflt) {
        return deflt;
    }

    @Override
    public double getDouble(String key, double deflt) {
        return deflt;
    }

    @Override
    public Map<String, String> prefixMatch(String prefix) {
        throw new NotImplementedException();
    }

    @Override
    public Object unwrapDelegate() {
        throw new NotImplementedException();
    }
}
