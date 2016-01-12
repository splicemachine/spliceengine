package com.splicemachine.access.api;

import java.util.List;
import java.util.Set;

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

        boolean hasStringDefault(String key);

        String defaultStringFor(String key);

        boolean defaultBooleanFor(String key);

        boolean hasBooleanDefault(String key);

        double defaultDoubleFor(String key);

        boolean hasDoubleDefault(String key);
    }

    void addDefaults(Defaults defaults);

    String getString(String key);

    /**
     * List all keys in the configuration which are set (or have a default) and start with the specified prefix.
     *
     * @param prefix the prefix to search for. An empty String or {@code null} will return all keys.
     * @return all keys which start with {@code prefix}
     */
    Set<String> prefixMatch(String prefix);

    double getDouble(String key);

    boolean getBoolean(String ignoreSavePoints);
}
