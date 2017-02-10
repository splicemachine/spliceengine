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

package com.splicemachine.derby.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.PropertyManager;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
@ThreadSafe
public class DirectPropertyManager implements PropertyManager{
    private final ConcurrentMap<String,String> properties= new ConcurrentHashMap<>();

    @Override
    public boolean propertyExists(String propertyName) throws StandardException{
        return properties.containsKey(propertyName);
    }

    @Override
    public Set<String> listProperties() throws StandardException{
        return properties.keySet();
    }

    @Override
    public String getProperty(String propertyName) throws StandardException{
        return properties.get(propertyName);
    }

    @Override
    public void addProperty(String propertyName,String propertyValue) throws StandardException{
        properties.put(propertyName,propertyValue);
    }

    @Override
    public void clearProperties() throws StandardException{
        properties.clear();
    }
}
