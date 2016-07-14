/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.storage.util;

import com.splicemachine.storage.Attributable;

import java.util.HashMap;
import java.util.Map;

/**
 * A simple class which holds attributes and does
 * nothing else. Useful when all you want is to keep
 * hold of some basic attributes but don't need
 * any storage-specific stuff.
 *
 * @author Scott Fines
 *         Date: 12/30/15
 */
public class MapAttributes implements Attributable{
    private final Map<String,byte[]> attrs = new HashMap<>();

    @Override
    public void addAttribute(String key,byte[] value){
        attrs.put(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return attrs.get(key);
    }

    @Override
    public Map<String, byte[]> allAttributes(){
        return attrs;
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        attrs.putAll(attrMap);
    }
}
