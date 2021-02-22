/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

import com.splicemachine.db.jdbc.EmbeddedDriver;


/**
 * Provides connections for internal use.
 */
public final class EmbedConnectionMaker2 {

    private static final String JDBC_URL = String.format("jdbc:splice:splicedb;user=splice;password=admin");

    private final EmbeddedDriver driver = new EmbeddedDriver();

    /**
     * Connection for internal use. Connects as 'splice' without using a password.
     */
//    @Override
    public Connection createNew(Properties dbProperties) throws SQLException {
        return createNew(JDBC_URL, dbProperties);
    }

    /**
     * Creates a Connection to the DB specified by jdbcString using dbProperties.
     * 
     * @param jdbcString
     * @param dbProperties
     * @return
     * @throws SQLException
     */
    public Connection createNew(String jdbcString, Properties dbProperties) throws SQLException {
        return driver.connect(jdbcString, dbProperties);
    }
}
