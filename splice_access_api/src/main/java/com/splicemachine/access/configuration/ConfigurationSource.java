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
