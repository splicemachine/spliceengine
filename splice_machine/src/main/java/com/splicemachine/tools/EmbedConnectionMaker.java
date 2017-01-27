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

package com.splicemachine.tools;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.SQLConfiguration;
import com.splicemachine.db.impl.jdbc.EmbedConnection;
import com.splicemachine.db.jdbc.EmbeddedDriver;


/**
 * Provides connections for internal use.
 */
public final class EmbedConnectionMaker {

    private static final String JDBC_URL = String.format("jdbc:splice:%s;user=splice;password=admin", SQLConfiguration.SPLICE_DB);

    private final EmbeddedDriver driver = new EmbeddedDriver();

    /**
     * Connection for internal use. Connects as 'splice' without using a password.
     */
//    @Override
    public Connection createNew(Properties dbProperties) throws SQLException {
        return driver.connect(JDBC_URL,dbProperties);
    }

    /**
     * Intended to be called the very first time we connect to a newly initialized database.  This
     * method appears to be responsible for setting the initial password for our 'splice' user.
     */
    public Connection createFirstNew(SConfiguration configuration,Properties dbProperties) throws SQLException {
        Properties properties = (Properties)dbProperties.clone();
        String auth = configuration.getAuthentication();
        if (!"LDAP".equalsIgnoreCase(auth)){
            properties.remove(EmbedConnection.INTERNAL_CONNECTION);
        }
        final String SPLICE_DEFAULT_PASSWORD = "admin";
        return driver.connect(JDBC_URL + ";create=true;password=" + SPLICE_DEFAULT_PASSWORD, properties);
    }
}
