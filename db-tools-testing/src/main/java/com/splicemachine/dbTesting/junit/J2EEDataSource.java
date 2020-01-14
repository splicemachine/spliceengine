/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.junit;

import java.util.HashMap;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

/**
 * Utility methods related to J2EE JDBC DataSource objects.
 * Separated out from JDBCDataSource to ensure that no
 * ClassNotFoundExceptions are thrown with JSR169.
 *
 */
public class J2EEDataSource {
    
    /**
     * Return a new DataSource corresponding to the current
     * configuration. The getPooledConnection() method is configured
     * to use the user name and password from the configuration.
     */
    public static javax.sql.ConnectionPoolDataSource getConnectionPoolDataSource()
    {
        return getConnectionPoolDataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getPooledConnection() method is configured
     * to use the user name and password from the configuration.
     */
    static ConnectionPoolDataSource getConnectionPoolDataSource(
            TestConfiguration config,
            HashMap beanProperties)
    {
        if (beanProperties == null)
             beanProperties = JDBCDataSource.getDataSourceProperties(config);
        
        String dataSourceClass = config.getJDBCClient().getConnectionPoolDataSourceClassName();
        
        return (ConnectionPoolDataSource) JDBCDataSource.getDataSourceObject(
                dataSourceClass, beanProperties);
    }
    
    /**
     * Return a new XA DataSource corresponding to the current
     * configuration. The getXAConnection() method is configured
     * to use the user name and password from the configuration.
     */
    public static XADataSource getXADataSource()
    {
        return getXADataSource(TestConfiguration.getCurrent(), (HashMap) null);
    }
    
    
    /**
     * Set a bean property for a data source. This code can be used
     * on any data source type.
     * @param ds DataSource to have property set
     * @param property name of property.
     * @param value Value, type is derived from value's class.
     */
    public static void setBeanProperty(Object ds, String property, Object value) {
       // reuse code from JDBCDataSource
        JDBCDataSource.setBeanProperty(ds, property, value);
    }
    
    /**
     * Create a new DataSource object setup from the passed in TestConfiguration.
     * The getXAConnection() method is configured
     * to use the user name and password from the configuration.
     */
    static XADataSource getXADataSource(TestConfiguration config,
            HashMap beanProperties)
    {
        if (beanProperties == null)
             beanProperties = JDBCDataSource.getDataSourceProperties(config);
        
        String dataSourceClass = config.getJDBCClient().getXADataSourceClassName();
        
        return (XADataSource) JDBCDataSource.getDataSourceObject(
                dataSourceClass, beanProperties);
    }
}
