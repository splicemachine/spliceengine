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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.dbTesting.junit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Connection factory using DriverManager.
 *
 */
public class DriverManagerConnector implements Connector {

    private TestConfiguration config;

    public DriverManagerConnector() {
    }

    public void setConfiguration(TestConfiguration config) {
        this.config = config;
    }

    public Connection openConnection() throws SQLException {
        return openConnection(config.getDefaultDatabaseName(), config.getUserName(), config.getUserPassword());
    }

    public Connection openConnection(String databaseName) throws SQLException {
        return openConnection(databaseName, config.getUserName(), config.getUserPassword());
    }

    public Connection openConnection(String user, String password) throws SQLException {
        return openConnection(config.getDefaultDatabaseName(), user, password);
    }

    /**
     * Open a connection using the DriverManager.
     * <BR>
     * The JDBC driver is only loaded if DriverManager.getDriver()
     * for the JDBC URL throws an exception indicating no driver is loaded.
     * <BR>
     * If the connection request fails with SQLState XJ004
     * (database not found) then the connection is retried
     * with attributes create=true.
     */
    public Connection openConnection(String databaseName, String user, String password)
            throws SQLException
    {
        return openConnection( databaseName, user, password, (Properties)  null );
    }
    
    /**
     * Open a connection using the DriverManager.
     * <BR>
     * The JDBC driver is only loaded if DriverManager.getDriver()
     * for the JDBC URL throws an exception indicating no driver is loaded.
     * <BR>
     * If the connection request fails with SQLState XJ004
     * (database not found) then the connection is retried
     * with attributes create=true.
     */
    public  Connection openConnection
        (String databaseName, String user, String password, Properties connectionProperties)
         throws SQLException
    {
        String url = config.getJDBCUrl(databaseName);

        try {
            DriverManager.getDriver(url);
        } catch (SQLException e) {
            loadJDBCDriver();
        }

        Properties connectionAttributes =
                new Properties(config.getConnectionAttributes());
        if ( user != null ) { connectionAttributes.setProperty("user", user); }
        if ( password  != null ) { connectionAttributes.setProperty("password", password); }

        if ( connectionProperties != null ) { connectionAttributes.putAll( connectionProperties ); }

        try {
            return DriverManager.getConnection(url, connectionAttributes);
        } catch (SQLException e) {

            // Expected state for database not found.
            // For the client the generic 08004 is returned,
            // will just retry on that.
            String expectedState = 
                config.getJDBCClient().isEmbedded() ? "XJ004" : "08004";

            // If there is a database not found exception
            // then retry the connection request with
            // a create attribute.
            if (!expectedState.equals(e.getSQLState()))
                throw e;
            
            Properties attributes = new Properties(connectionAttributes);
            attributes.setProperty("create", "true");

            return DriverManager.getConnection(url, attributes);
        }
    }

    /**
     * Shutdown the database using the attributes shutdown=true
     * with the user and password defined by the configuration.
     */
    public void shutDatabase() throws SQLException {
        getConnectionByAttributes(config.getJDBCUrl(),
                "shutdown", "true");
    }

    /**
     * Shutdown the engine using the attributes shutdown=true
     * and no database name with the user and password defined
     * by the configuration.
     * Always shutsdown using the embedded URL thus this
     * method will not work in a remote testing environment.
     */
    public void shutEngine() throws SQLException {
        
        getConnectionByAttributes("jdbc:splice:", "shutdown", "true");
    }
    
    /**
     * Open a connection using JDBC attributes with a JDBC URL.
     * The attributes user and password are set from the configuration
     * and then the passed in attribute is set.
     */
    private Connection getConnectionByAttributes(String url, String key, String value)
        throws SQLException
    {
        Properties attributes = new Properties();

        attributes.setProperty("user", config.getUserName());
        attributes.setProperty("password", config.getUserPassword());
        attributes.setProperty(key, value);

        try {
            DriverManager.getDriver(url);
        } catch (SQLException e) {
            loadJDBCDriver();
        }

        return DriverManager.getConnection(url, attributes);
    }

    /**
     * Load the JDBC driver defined by the JDBCClient for
     * the configuration.
     *
     * @throws SQLException if loading the driver fails.
     */
    private void loadJDBCDriver() throws SQLException {
        String driverClass = config.getJDBCClient().getJDBCDriverName();
        try {
            Class.forName(driverClass).newInstance();
        } catch (ClassNotFoundException cnfe) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + cnfe.getMessage());
        } catch (IllegalAccessException iae) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + iae.getMessage());
        } catch (InstantiationException ie) {
            throw new SQLException("Failed to load JDBC driver '" + driverClass
                    + "': " + ie.getMessage());
        }
    }
}
