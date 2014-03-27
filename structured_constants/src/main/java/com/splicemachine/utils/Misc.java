package com.splicemachine.utils;

import com.google.common.base.Supplier;

import java.util.Map;

/**
 * @author P Trolard
 *         Date: 20/03/2014
 */
public class Misc {
    // Miscellaneous utilities

    /**
     * Return value of key k in map m; if k not found, insert value supplied by
     * defaultVal for k and return value.
     */
    public static <K,V> V lookupOrDefault(Map<K,V> m, K k, Supplier<V> defaultVal){
        V val = m.get(k);
        if (val == null){
            val = defaultVal.get();
            m.put(k, val);
        }
        return val;
    }
}
