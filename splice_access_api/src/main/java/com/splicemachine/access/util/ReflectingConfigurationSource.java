/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
