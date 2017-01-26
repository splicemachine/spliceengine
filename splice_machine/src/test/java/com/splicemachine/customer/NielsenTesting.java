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

package com.splicemachine.customer;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.Properties;

import org.apache.log4j.Logger;

public class NielsenTesting {

    public static String testInternalConnection() throws Exception {
        Connection mconn = DriverManager
                .getConnection("jdbc:default:connection");
        String lv_ref_val = null;
        try {
            if (mconn != null) {
                lv_ref_val = "Got an internal connection";
            }
            else {
                lv_ref_val = "Cannot get an internal connection";
            }
        } finally {
            if (mconn != null) {
                mconn.close();
            }
        }
        return lv_ref_val;
    }
}
