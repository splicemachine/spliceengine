package com.splicemachine.access.api;

/**
 * Generic interface for representing a Configuration.
 *
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface SConfiguration{

    /**
     * @param key the label for this configuration
     * @return the value for the configuration associated with {@code key}. If there is
     * an overridden(i.e. manually set) configuration, then return that. If there is no overridden
     * method, but there is a default, return that value. If there is no default and no specifically
     * set value, throw an error
     * @throws IllegalArgumentException if there is no overridden or default value available
     * for the specified key.
     */
    long getLong(String key);

    /**
     * @param key the label for this configuration
     * @return the value for the configuration associated with {@code key}. If there is
     * an overridden(i.e. manually set) configuration, then return that. If there is no overridden
     * method, but there is a default, return that value. If there is no default and no specifically
     * set value, throw an error
     * @throws IllegalArgumentException if there is no overridden or default value available
     * for the specified key.
     */
    int getInt(String key);

    interface Defaults{

        boolean hasLongDefault(String key);

        long defaultLongFor(String key);

        boolean hasIntDefault(String key);

        int defaultIntFor(String key);
    }
}
