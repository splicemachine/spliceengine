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

package com.splicemachine.derby.test.framework;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Convenient factory for obtaining java.sql.Connection to LOCAL splice from tests.
 */
public class SpliceNetConnection {

    public static final String DB_URL_LOCAL = "jdbc:splice://localhost:1527/splicedb;create=true;user=%s;password=%s;clustered=false";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    public static Connection getConnection() throws SQLException {
        return getConnectionAs(DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    public static Connection getConnectionAs(String userName, String password) throws SQLException {
        return getConnectionAs(DB_URL_LOCAL, userName, password);
    }

    public static String getDefaultLocalURL() {
        return String.format(DB_URL_LOCAL, DEFAULT_USER, DEFAULT_USER_PASSWORD);
    }

    private static Connection getConnectionAs(String providedURL, String userName, String password) throws SQLException {
        String url = String.format(providedURL, userName, password);
        return DriverManager.getConnection(url, new Properties());
    }

}