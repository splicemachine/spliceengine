package com.splicemachine.derby.iapi.sql;

import com.splicemachine.db.iapi.error.StandardException;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
@ThreadSafe
public interface PropertyManager{

    boolean propertyExists(String propertyName) throws StandardException;

    Set<String> listProperties() throws StandardException;

    String getProperty(String propertyName) throws StandardException;

    void addProperty(String propertyName, String propertyValue) throws StandardException;

    void clearProperties() throws StandardException;
}
