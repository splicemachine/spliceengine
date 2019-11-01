package com.splicemachine.client;

import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.jdbc.ClientDriver;
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

    @Test
    public void testParseProperty() throws Exception {
        String url = "jdbc:splice://localhost.com:1527/splicedb;user=af29891;password=admin";
        ClientDriver driver = new ClientDriver();
        java.sql.DriverPropertyInfo[] propertyInfos = driver.getPropertyInfo(url, null);
        assertEquals(propertyInfos[0].name, "user");
        assertEquals(propertyInfos[0].value, "af29891");

        assertEquals(propertyInfos[1].name, "password");
        assertEquals(propertyInfos[1].value, "admin");
    }
}
