package com.splicemachine.db.impl.token;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import io.jsonwebtoken.Jwts;
import org.junit.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.sql.*;
import java.util.*;

public class TokenLoginIT extends SpliceUnitTest {

    public static final String TEST_USER_NAME = "splice_token_test";

    public static final String SPLICEMACHINE_COM_ISSUER = "http://splicemachine.com/";

    @BeforeClass
    public static void setup() throws SQLException {
        Statement s = SpliceNetConnection.getDefaultConnection().createStatement();
        s.execute(String.format("call SYSCS_UTIL.SYSCS_CREATE_USER('%s','test')", TEST_USER_NAME));
    }

    @AfterClass
    public static void cleanup() throws SQLException {
        Statement s = SpliceNetConnection.getDefaultConnection().createStatement();
        s.execute(String.format("call SYSCS_UTIL.SYSCS_DROP_USER('%s')", TEST_USER_NAME));
        s.execute(String.format("DROP SCHEMA %s CASCADE", TEST_USER_NAME.toUpperCase()));
    }

    @Test
    @Ignore
    public void testLoginWithOktaToken() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=%s;authenticator=okta_oauth;token=%s";
        String userName = TEST_USER_NAME;
        String newOktaToken = "eyJraWQiOiJlR2I0R1o0VGVRejZzV0dtU3otY1NtcFNuR0JtbUlEUlVweVlDMTdkemljIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIwMHUzM2hramZhRVdHQUtpYjVkNiIsIm5hbWUiOiJJZ29yIFByYXpuaWsiLCJlbWFpbCI6InByYXpuaWtpZ29yQHlhaG9vLmNvbSIsInZlciI6MSwiaXNzIjoiaHR0cHM6Ly9kZXYtOTUwMzE4Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6IjBvYTMzaHk3YzdmRXlKQUdjNWQ2IiwiaWF0IjoxNjEwMzYzMTQ3LCJleHAiOjE2MTAzNjY3NDcsImp0aSI6IklELllHdGx0MmowTnBpdHhNUHc0MHRZMkdmMzRPbFVvUVpCTm1oQ20zR3BwOG8iLCJhbXIiOlsicHdkIl0sImlkcCI6IjAwbzMzaGtmbXc4d0Flczg1NWQ2Iiwibm9uY2UiOiIzMVRpWlkwenV0eDU4a3dQM0Z5WVAyY2FPVUpTVVBGcWUyUF85OUU1U2tvIiwicHJlZmVycmVkX3VzZXJuYW1lIjoicHJhem5pa2lnb3JAeWFob28uY29tIiwiYXV0aF90aW1lIjoxNjEwMzYzMTQ1LCJhdF9oYXNoIjoiQ2c5MEEwZ3RCSjFEUjgtdXBjQ2ZHZyIsImRiX3VzZXIiOiJpZ29yIn0.G7g5lIRN0JkCFFmqFhbXlyB5I1cJ-NXNij1kQp1la31zJ9XkJhsszlV-wyhbIrPfY4zfkkpNBrMxiE1zhajQNOck5J12LEQS49GyVJd8Eu685TXtyYy-ZnwUx5eBKZEWC4OEZMsAE--NsyqUP3nwyqj2fwD_Jh4bEnlAtwVPjDKMS9i1eBLKAwv83aTDjXoXTQUam3qKLxMJwOEWXgzMBX8JavZL6cggtFKiPV-Arrwpw2eZ4GuXsyWyRNmZ3azsAnIZ2kJLntm7nIJNEp6JfdEQJiGwt-twOCRPfDUT0KdlylIVrYooFhXxcyUI9Gp4TMMjqRxgBuckEgL2pCWw8Q";
        testTokeUserDatabase(url, userName, newOktaToken);
    }

    @Test
    public void testLoginWithSpliceKey() throws SQLException {
        String url = "jdbc:splice://localhost:1527/splicedb;user=%s;authenticator=splice_jwt_pub;token=%s";
        String userName = TEST_USER_NAME;
        String newToken = createNewSpliceToken();
        testTokeUserDatabase(url, userName, newToken);
    }

    private void testTokeUserDatabase(String url, String userName, String newToken) throws SQLException {
        try (Connection spliceConnection = DriverManager.getConnection(String.format(url, userName, newToken)); Statement statement = spliceConnection.createStatement()) {

            ResultSet rs = null;
            List<String> schemas = new ArrayList<>();

            DatabaseMetaData dbmd = spliceConnection.getMetaData();
            rs = dbmd.getSchemas();

            while (rs.next()) {
                schemas.add(rs.getString(1));
            }

            Assert.assertEquals(2, schemas.size());
            Assert.assertTrue( schemas.contains(userName.toUpperCase()));

            //Create a table
            statement.execute("CREATE TABLE MYTESTTABLE(a int, b varchar(30))");

            //Insert data into the table
            statement.execute("insert into MYTESTTABLE values (1,'a')");
            statement.execute("insert into MYTESTTABLE values (2,'b')");
            statement.execute("insert into MYTESTTABLE values (3,'c')");
            statement.execute("insert into MYTESTTABLE values (4,'c')");
            statement.execute("insert into MYTESTTABLE values (5,'c')");

            rs = statement.executeQuery("explain select * from MYTESTTABLE");
            Assert.assertTrue(rs.next());

        }
    }

    private String createNewSpliceToken() {
        Key key = loadKey(TokenLoginIT.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database");
        String jwt = createJWT(key);
        return jwt;
    }

    private static Key loadKey(String keyStorePath, String password, String alias) {
        try {
            KeyStore keyStore = KeyStore.getInstance("pkcs12");
            try(InputStream keyStoreData = new FileInputStream(keyStorePath)){
                keyStore.load(keyStoreData, password.toCharArray());
                Key key = keyStore.getKey(alias, password.toCharArray());
                return key;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private static String createJWT(Key key) {
        return  createJWT(key, 60);
    }

    private static String createJWT(Key key, int expiration) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.SECOND, expiration);  // number of days to add

        Map<String, Object> additionalClaims = new HashMap<>();
        additionalClaims.put("db_user", TEST_USER_NAME);

        return Jwts.builder().setIssuer(SPLICEMACHINE_COM_ISSUER)
                .setSubject(TEST_USER_NAME)
                .setExpiration(c.getTime())
                .setAudience("database")
                .addClaims(additionalClaims)
                .signWith(key).compact();
    }

}
