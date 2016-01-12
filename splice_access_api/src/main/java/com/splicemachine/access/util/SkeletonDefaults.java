package com.splicemachine.access.util;

import com.splicemachine.access.api.SConfiguration;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public abstract class SkeletonDefaults implements SConfiguration.Defaults{
    @Override
    public boolean hasLongDefault(String key){
        return false;
    }

    @Override
    public long defaultLongFor(String key){
        throw new IllegalStateException("no default long value for key '"+key+"'");
    }

    @Override
    public boolean hasIntDefault(String key){
        return false;
    }

    @Override
    public int defaultIntFor(String key){
        throw new IllegalStateException("no default int value for key '"+key+"'");
    }

    @Override
    public boolean hasStringDefault(String key){
        return false;
    }

    @Override
    public String defaultStringFor(String key){
        throw new IllegalStateException("no default string value for key '"+key+"'");
    }

    @Override
    public boolean defaultBooleanFor(String key){
        throw new IllegalStateException("no default boolean value for key '"+key+"'");
    }

    @Override
    public boolean hasBooleanDefault(String key){
        return false;
    }

    @Override
    public double defaultDoubleFor(String key){
        throw new IllegalStateException("no default double value for key '"+key+"'");
    }

    @Override
    public boolean hasDoubleDefault(String key){
        return false;
    }
}
