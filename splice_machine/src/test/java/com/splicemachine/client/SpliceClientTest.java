package com.splicemachine.client;

import com.splicemachine.db.client.am.SqlException;
import org.junit.Test;

import static org.junit.Assert.*;

public class SpliceClientTest {

    @Test
    public void testParseJDBCPassword() throws SqlException {
        String password = "Abd98*@80EFg";
        String raw = "jdbc:splice://localhost.com:1527/splicedb;user=af29891;password=" + password;
        assertEquals(password, SpliceClient.parseJDBCPassword(raw));

        raw = "jdbc:splice://localhost:1527/splicedb;password=" + password + ";user=af29891";
        assertEquals(password, SpliceClient.parseJDBCPassword(raw));
    }
}
