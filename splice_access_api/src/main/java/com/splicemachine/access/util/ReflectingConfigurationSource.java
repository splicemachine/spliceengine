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

package com.splicemachine.access.util;

import java.util.Map;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import com.splicemachine.access.configuration.ConfigurationSource;

/**
 * Mocked up ConfigurationSource for testing. You get what you give it.
 */
public class ReflectingConfigurationSource implements ConfigurationSource {
    @Override
    public int getInt(String key, int deflt) {
        return deflt;
    }

    @Override
    public long getLong(String key, long deflt) {
        return deflt;
    }

    @Override
    public boolean getBoolean(String key, boolean deflt) {
        return deflt;
    }

    @Override
    public String getString(String key, String deflt) {
        return deflt;
    }

    @Override
    public double getDouble(String key, double deflt) {
        return deflt;
    }

    @Override
    public Map<String, String> prefixMatch(String prefix) {
        throw new NotImplementedException();
    }

    @Override
    public Object unwrapDelegate() {
        throw new NotImplementedException();
    }
}
