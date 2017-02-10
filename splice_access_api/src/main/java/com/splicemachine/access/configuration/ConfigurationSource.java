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

package com.splicemachine.access.configuration;

import java.util.Map;

/**
 * The source of configuration properties.
 * <p/>
 * This interface exists solely to eliminate a compile-time dependency on our source of configuration
 * properties, which for us is hadoop Configuration and this interface defines the methods from that
 * which we used.  Doing that also allows us to create our own for testing.
 */
public interface ConfigurationSource {

    /**
     * Get the value from this configuration source set by the given <code>key</code>
     * or <code>deflt</code> if no value exists for that key.
     * @param key the property key
     * @param deflt the default value to return if no value exists for the given key
     * @return the value that was set or the default value if no value exists yet
     */
    int getInt(String key, int deflt);

    /**
     * Get the value from this configuration source set by the given <code>key</code>
     * or <code>deflt</code> if no value exists for that key.
     * @param key the property key
     * @param deflt the default value to return if no value exists for the given key
     * @return the value that was set or the default value if no value exists yet
     */
    long getLong(String key, long deflt);

    /**
     * Get the value from this configuration source set by the given <code>key</code>
     * or <code>deflt</code> if no value exists for that key.
     * @param key the property key
     * @param deflt the default value to return if no value exists for the given key
     * @return the value that was set or the default value if no value exists yet
     */
    boolean getBoolean(String key, boolean deflt);

    /**
     * Get the value from this configuration source set by the given <code>key</code>
     * or <code>deflt</code> if no value exists for that key.
     * @param key the property key
     * @param deflt the default value to return if no value exists for the given key
     * @return the value that was set or the default value if no value exists yet
     */
    String getString(String key, String deflt);

    /**
     * Get the value from this configuration source set by the given <code>key</code>
     * or <code>deflt</code> if no value exists for that key.
     * @param key the property key
     * @param deflt the default value to return if no value exists for the given key
     * @return the value that was set or the default value if no value exists yet
     */
    double getDouble(String key, double deflt);

    /**
     * Get the mapping of property key to (toString()) value for configuration properties
     * that start with the given prefix.<br/>
     * If <code>null</code> is passed as an argument, all properties are returned.
     * <p/>
     * To get all splice DDL properties, for example:
     * <pre>
     *     Map<String,String> props = config.prefixMatch("splice.ddl*");
     * </pre>
     * @param prefix the property's starting key
     * @return all property key/value pairs for keys that start with <code>prefix</code>.
     */
    Map<String, String> prefixMatch(String prefix);

    /**
     * Get the source configuration object.
     * @return the source configuration, untyped to eliminate the compile-time dependency.
     */
    Object unwrapDelegate();
}
