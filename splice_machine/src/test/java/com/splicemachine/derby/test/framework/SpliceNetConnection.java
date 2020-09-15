/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
 * Convenient factory for obtaining java.sql.Connection to Splice from tests.
 */
public class SpliceNetConnection {

    public static final String PROPERTY_JDBC_URL = "splice.jdbc.url";
    public static final String PROPERTY_JDBC_HOST = "splice.jdbc.host";
    public static final String PROPERTY_JDBC_PORT = "splice.jdbc.port";
    public static final String PROPERTY_JDBC_USER = "splice.jdbc.user";
    public static final String PROPERTY_JDBC_PASSWORD = "splice.jdbc.password";
    public static final String PROPERTY_JDBC_SSL = "splice.jdbc.ssl";

    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = "1527";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    private static String jdbcURL;
    private static String jdbcUser = DEFAULT_USER;
    private static String jdbcPassword = DEFAULT_USER_PASSWORD;
    private static String jdbcSSL = null;

    static {
        String url = System.getProperty(PROPERTY_JDBC_URL);
        if (url != null) {
            int idx = url.indexOf(";");
            if (idx < 0) {
                jdbcURL = url;
            }
            else {
                final String PARAM_USER = "user=";
                final String PARAM_PASSWORD = "password=";
                final String PARAM_SSL = "ssl=";

                jdbcURL = url.substring(0, idx);
                String args = url.substring(idx + 1);
                while (args.length() > 0) {
                    String arg;
                    idx = args.indexOf(";");
                    if (idx == -1) {
                        arg = args;
                        args = "";
                    }
                    else {
                        arg = args.substring(0, idx);
                        args = args.substring(idx + 1);
                    }

                    if (arg.startsWith(PARAM_USER)) {
                        jdbcUser = arg.substring(PARAM_USER.length());
                    }
                    else if (arg.startsWith(PARAM_PASSWORD)) {
                        jdbcPassword = arg.substring(PARAM_PASSWORD.length());
                    }
                    else if (arg.startsWith(PARAM_SSL)) {
                        jdbcSSL = arg.substring(PARAM_SSL.length());
                    }
                }
            }
        }
        else {
            String host = System.getProperty(PROPERTY_JDBC_HOST, DEFAULT_HOST);
            String port = System.getProperty(PROPERTY_JDBC_PORT, DEFAULT_PORT);
            jdbcURL = "jdbc:splice://" + host + ":" + port + "/splicedb";
        }
        jdbcUser = System.getProperty(PROPERTY_JDBC_USER, jdbcUser);
        jdbcPassword = System.getProperty(PROPERTY_JDBC_PASSWORD, jdbcPassword);
        jdbcSSL = System.getProperty(PROPERTY_JDBC_SSL, jdbcSSL);
    }

    public static Connection getConnection() throws SQLException {
        return getConnectionAs(jdbcUser, jdbcPassword);
    }

    public static Connection getConnectionAs(String userName, String password) throws SQLException {
        return getConnectionAs(jdbcURL, userName, password);
    }

    public static Connection getConnectionAs(String userName, String password, boolean useSpark) throws SQLException {
        Properties info = new Properties();
        info.put("user", userName);
        info.put("password", password);
        info.put("useSpark", Boolean.toString(useSpark));
        if (jdbcSSL != null) {
            info.put("ssl", jdbcSSL);
        }
        return DriverManager.getConnection(jdbcURL, info);
    }

    public static String getDefaultLocalURL() {
        StringBuilder sb = new StringBuilder(jdbcURL);
        sb.append(";user=").append(jdbcUser);
        sb.append(";password=").append(jdbcPassword);
        if (jdbcSSL != null) {
            sb.append(";ssl=").append(jdbcSSL);
        }
        return sb.toString();
    }

    public static Connection getConnectionAs(String providedURL, String userName, String password) throws SQLException {
        Properties info = new Properties();
        info.put("user", userName);
        info.put("password", password);
        if (jdbcSSL != null) {
            info.put("ssl", jdbcSSL);
        }
        return DriverManager.getConnection(providedURL, info);
    }

}
