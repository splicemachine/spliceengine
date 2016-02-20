package com.splicemachine.derby.security;

import com.splicemachine.constants.SpliceConstants;
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
            Assert.assertTrue(e.getSQLState().compareTo("08004")==0);
        }
    }
}
