package com.splicemachine.utils;

import java.util.Map;

/**
 * @author P Trolard
 *         Date: 20/03/2014
 */
public class Misc {
    // Miscellaneous utilities

    public static <K,V> V lookupOrDefault(Map<K,V> m, K k, V defaultVal){
        V val = m.get(k);
        if (val == null){
            m.put(k, defaultVal);
            return defaultVal;
        }
        return val;
    }
}
