package com.splicemachine.storage;

import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public interface Attributable{

    void addAttribute(String key, byte[] value);

    byte[] getAttribute(String key);

    Map<String,byte[]> allAttributes();

    void setAllAttributes(Map<String,byte[]> attrMap);
}
