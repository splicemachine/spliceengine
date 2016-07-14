/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.access;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.splicemachine.access.configuration.ConfigurationSource;

/**
 * An instance of {@link ConfigurationSource} that wraps {@link Configuration}.<br/>
 * @see #unwrapDelegate()
 */
public class HBaseConfigurationSource implements ConfigurationSource {
    private final Configuration delegate;

    public HBaseConfigurationSource(Configuration delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getInt(String key, int deflt) {
        return delegate.getInt(key, deflt);
    }

    @Override
    public long getLong(String key, long deflt) {
        return delegate.getLong(key, deflt);
    }

    @Override
    public boolean getBoolean(String key, boolean deflt) {
        return delegate.getBoolean(key, deflt);
    }

    @Override
    public String getString(String key, String deflt) {
        String value = delegate.get(key);
        return (value != null ? value : deflt);
    }

    @Override
    public double getDouble(String key, double deflt) {
        return delegate.getDouble(key, deflt);
    }

    @Override
    public Map<String, String> prefixMatch(String prefix){
        return delegate.getValByRegex(String.format("^%s", prefix == null ? "" : prefix));
    }

    /**
     * Since everything under us, HBase, Spark, Hadoop, etc., uses {@link Configuration},
     * we must expose a way to get the configured delegate.
     * <p/>
     * This method is not exposed in the interface, of course, because it would create a
     * compile-time dependency on {@link Configuration}. Given that, it's a little convoluted
     * to call this method.  It's assumed that callers have access to this method can also
     * support a compile-time dependency on the hadoop configuration.
     * <p/>
     * Example:
     * <pre>
     *  Configuration config = ((HBaseConfigurationSource)HConfiguration.getConfigSource()).unwrapDelegate()
     * </pre>
     * @return the underlying hadoop configuration.
     */
    public Configuration unwrapDelegate() {
        return delegate;
    }
}
