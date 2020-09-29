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
    public static final String PROPERTY_JDBC_DATABASE = "splice.jdbc.database";
    public static final String PROPERTY_JDBC_HOST = "splice.jdbc.host";
    public static final String PROPERTY_JDBC_PORT = "splice.jdbc.port";
    public static final String PROPERTY_JDBC_USER = "splice.jdbc.user";
    public static final String PROPERTY_JDBC_PASSWORD = "splice.jdbc.password";
    public static final String PROPERTY_JDBC_SSL = "splice.jdbc.ssl";
    public static final String PROPERTY_JDBC_CREATE = "splice.jdbc.create";
    public static final String PROPERTY_JDBC_SCHEMA = "splice.jdbc.schema";
    public static final String PROPERTY_JDBC_USEOLAP = "splice.jdbc.useolap";

    public static final String URL_PREFIX = "jdbc:splice://";
    public static final String DEFAULT_HOST = "localhost";
    public static final String DEFAULT_PORT = "1527";
    public static final String DEFAULT_DATABASE = "splicedb";
    public static final String DEFAULT_USER = "splice";
    public static final String DEFAULT_USER_PASSWORD = "admin";

    private static String jdbcUrl;
    private static String jdbcHost;
    private static String jdbcPort;
    private static String jdbcDatabase;
    private static String jdbcUser = DEFAULT_USER;
    private static String jdbcPassword = DEFAULT_USER_PASSWORD;
    private static String jdbcSsl = null;
    private static String jdbcCreate = null;
    private static String jdbcSchema = null;
    private static String jdbcUseOLAP = null;

    private static void separateUrlAndDatabase(String url) {
        int idx = url.lastIndexOf("/");
        jdbcUrl = url.substring(0, idx);
        jdbcDatabase = url.substring(idx + 1);
    }

    static {
        String url = System.getProperty(PROPERTY_JDBC_URL);
        if (url != null) {
            int idx = url.indexOf(";");
            if (idx < 0) {
                separateUrlAndDatabase(url);
            }
            else {
                final String PARAM_USER = "user=";
                final String PARAM_PASSWORD = "password=";
                final String PARAM_SSL = "ssl=";
                final String PARAM_CREATE = "create=";
                final String PARAM_USEOLAP = "useOLAP=";
                final String PARAM_USESPARK = "useSpark=";

                separateUrlAndDatabase(url.substring(0, idx));
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
                        jdbcSsl = arg.substring(PARAM_SSL.length());
                    }
                    else if (arg.startsWith(PARAM_CREATE)) {
                        jdbcCreate = arg.substring(PARAM_CREATE.length());
                    }
                    else if (arg.startsWith(PARAM_USEOLAP)) {
                        jdbcUseOLAP = arg.substring(PARAM_USEOLAP.length());
                    }
                    else if (arg.startsWith(PARAM_USESPARK)) {
                        jdbcUseOLAP = arg.substring(PARAM_USESPARK.length());
                    }
                }
            }
        }
        else {
            jdbcHost = System.getProperty(PROPERTY_JDBC_HOST, DEFAULT_HOST);
            jdbcPort = System.getProperty(PROPERTY_JDBC_PORT, DEFAULT_PORT);
            jdbcUrl = URL_PREFIX + jdbcHost + ":" + jdbcPort;
            jdbcDatabase = System.getProperty(PROPERTY_JDBC_DATABASE, DEFAULT_DATABASE);
        }
        jdbcUser = System.getProperty(PROPERTY_JDBC_USER, jdbcUser);
        jdbcPassword = System.getProperty(PROPERTY_JDBC_PASSWORD, jdbcPassword);
        jdbcSsl = System.getProperty(PROPERTY_JDBC_SSL, jdbcSsl);
        jdbcCreate = System.getProperty(PROPERTY_JDBC_CREATE, jdbcCreate);
        jdbcSchema = System.getProperty(PROPERTY_JDBC_SCHEMA, jdbcSchema);
        jdbcUseOLAP = System.getProperty(PROPERTY_JDBC_USEOLAP, jdbcUseOLAP);
    }

    public static class ConnectionBuilder implements Cloneable {
        private String host;
        private String port;
        private String database;
        private String create;
        private String user;
        private String password;
        private String schema;
        private String ssl;
        private String useOLAP;

        @Override
        public ConnectionBuilder clone() throws CloneNotSupportedException {
            return (ConnectionBuilder) super.clone();
        }

        public ConnectionBuilder host(String host) {
            this.host = host;
            return this;
        }
        public ConnectionBuilder port(int port) {
            this.port = Integer.toString(port);
            return this;
        }
        public ConnectionBuilder database(String database) {
            this.database = database;
            return this;
        }
        public ConnectionBuilder create(boolean create) {
            this.create = Boolean.toString(create);
            return this;
        }
        public ConnectionBuilder user(String user) {
            this.user = user;
            return this;
        }
        public ConnectionBuilder password(String password) {
            this.password = password;
            return this;
        }
        public ConnectionBuilder schema(String schema) {
            this.schema = schema;
            return this;
        }
        public ConnectionBuilder ssl(boolean ssl) {
            this.ssl = Boolean.toString(ssl);
            return this;
        }
        public ConnectionBuilder useOLAP(boolean useOLAP) {
            this.useOLAP = Boolean.toString(useOLAP);
            return this;
        }

        public Connection build() throws SQLException {
            Properties info = new Properties();
            info.put("user", user != null ? user : jdbcUser);
            info.put("password", password != null ? password : jdbcPassword);
            if (create != null || jdbcCreate != null)
                info.put("create", create != null ? create : jdbcCreate);
            if (schema != null || jdbcSchema != null)
                info.put("schema", schema != null ? schema : jdbcSchema);
            if (ssl != null || jdbcSsl != null)
                info.put("ssl", (ssl != null ? ssl : jdbcSsl));
            if (useOLAP != null || jdbcUseOLAP != null)
                info.put("useOLAP", useOLAP != null ? useOLAP : jdbcUseOLAP);
            StringBuilder url = new StringBuilder();
            if (host != null || port != null) {
                url.append(URL_PREFIX);
                url.append(host != null ? host : jdbcHost);
                url.append(":");
                url.append(port != null ? port : jdbcPort);
            } else {
                url.append(jdbcUrl);
            }
            url.append("/");
            url.append(database != null ? database: jdbcDatabase);
            return DriverManager.getConnection(url.toString(), info);
        }
    }

    public static ConnectionBuilder newBuilder() {
        return new ConnectionBuilder();
    }

    public static Connection getDefaultConnection() throws SQLException {
        return newBuilder().build();
    }

    public static String getDefaultLocalURL() {
        StringBuilder sb = new StringBuilder(jdbcUrl);
        sb.append("/").append(jdbcDatabase);
        sb.append(";user=").append(jdbcUser);
        sb.append(";password=").append(jdbcPassword);
        if (jdbcSsl != null) {
            sb.append(";ssl=").append(jdbcSsl);
        }
        if (jdbcCreate != null) {
            sb.append(";create=").append(jdbcCreate);
        }
        return sb.toString();
    }
}
