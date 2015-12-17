package com.splicemachine.si.impl.data.light;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jleach on 12/10/15.
 */
public class LOperationWithAttributes {
    // An opaque blob of attributes
    private Map<String, byte[]> attributes;

    public void setAttribute(String name, byte[] value) {
        if (attributes == null && value == null) {
            return;
        }

        if (attributes == null) {
            attributes =new HashMap<>();
        }

        if (value == null) {
            attributes.remove(name);
            if (attributes.isEmpty()) {
                this.attributes = null;
            }
        } else {
            attributes.put(name, value);
        }
    }

    public byte[] getAttribute(String name) {
        if (attributes == null) {
            return null;
        }

        return attributes.get(name);
    }

    public Map<String, byte[]> getAttributesMap() {
        if (attributes == null) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(attributes);
    }

}
