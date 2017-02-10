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

package com.splicemachine.derby.test.framework;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Convenient factory for obtaining java.sql.Connection to LOCAL splice from tests.
 */
public class SpliceNetConnection {

    private static final String DB_URL_LOCAL = "jdbc:splice://localhost:1527/splicedb;create=true;user=%s;password=%s";
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

    public static Connection getConnectionAs(String providedURL, String userName, String password) throws SQLException {
        String url = String.format(providedURL, userName, password);
        return DriverManager.getConnection(url, new Properties());
    }

}