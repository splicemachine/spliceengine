/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
