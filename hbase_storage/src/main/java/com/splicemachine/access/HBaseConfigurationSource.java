/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
    public int getInt(String key, int defaultValue) {
        try {
            return delegate.getInt(key, defaultValue);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public int getInt(String key, int defaultValue, int minValue, int maxValue) {
        int i = getInt(key, defaultValue);
        if (i < minValue)
            return minValue;
        if (i > maxValue)
            return maxValue;
        return i;
    }

    @Override
    public long getLong(String key, long defaultValue) {
        try {
            return delegate.getLong(key, defaultValue);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public long getLong(String key, long defaultValue, long minValue, long maxValue) {
        long l = getLong(key, defaultValue);
        if (l < minValue)
            return minValue;
        if (l > maxValue)
            return maxValue;
        return l;
    }

    @Override
    public boolean getBoolean(String key, boolean defaultValue) {
        return delegate.getBoolean(key, defaultValue);
    }

    @Override
    public String getString(String key, String defaultValue) {
        String value = delegate.get(key);
        return (value != null ? value : defaultValue);
    }

    @Override
    public double getDouble(String key, double defaultValue) {
        try {
            return delegate.getDouble(key, defaultValue);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    @Override
    public double getDouble(String key, double defaultValue, double minValue, double maxValue) {
        double d = getDouble(key, defaultValue);
        if (d < minValue)
            return minValue;
        if (d > maxValue)
            return maxValue;
        return d;
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
