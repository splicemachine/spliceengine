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

package com.splicemachine.derby.security;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUserWatcher;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.SQLNonTransientConnectionException;
import java.util.Properties;

import static org.junit.Assert.fail;

public class AuthenticationIT {

    private static final String AUTH_IT_USER = "auth_it_user";
    private static final String AUTH_IT_PASS = "test_password";

    @Rule
    public SpliceUserWatcher spliceUserWatcher1 = new SpliceUserWatcher(AUTH_IT_USER, AUTH_IT_PASS);

    @Test
    public void valid() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS);
    }

    @Test
    public void validUsernameIsNotCaseSensitive() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER.toUpperCase(), AUTH_IT_PASS);
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad password
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPassword() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "bad_password");
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtStart() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "a" + AUTH_IT_PASS);
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordExtraCharAtEnd() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS + "a");
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordCase() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, AUTH_IT_PASS.toUpperCase());
    }

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badPasswordZeroLength() throws SQLException {
        SpliceNetConnection.getConnectionAs(AUTH_IT_USER, "");
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // bad username
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    @Test(expected = SQLNonTransientConnectionException.class)
    public void badUsername() throws SQLException {
        SpliceNetConnection.getConnectionAs("bad_username", AUTH_IT_PASS);
    }


    //DB-4618
    @Test
    public void invalidDbname() throws SQLException {
        String url = "jdbc:splice://localhost:1527/anotherdb;user=user;password=passwd";
        try {
            DriverManager.getConnection(url, new Properties());
            fail("Expected authentication failure");
        } catch (SQLNonTransientConnectionException e) {
            Assert.assertTrue(e.getSQLState().compareTo("08004") == 0);
        }
    }
}
