package com.splicemachine.derby.client;

import org.junit.Test;

import java.sql.*;

/**
 * Created by jleach on 12/13/16.
 */
public class TestUnknownClient {

    @Test (expected = SQLNonTransientConnectionException.class)
    public void testDerbyDriverFails() throws Exception {
        Class.forName("org.apache.derby.jdbc.ClientDriver").newInstance();
        Connection conn = DriverManager.getConnection("jdbc:derby://localhost:1527/splicedb;create=true;user=splice;password=admin");
    }

}
