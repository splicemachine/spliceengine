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
