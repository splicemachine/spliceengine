package com.splicemachine.db.impl.token;

import com.splicemachine.db.iapi.error.StandardException;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import io.jsonwebtoken.security.Keys;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class KeyAccessTokenVerifierTest {

    public static final String SPLICEMACHINE_COM_ISSUER = "http://splicemachine.com/";

    @Test
    public void testKeyAccessTokenVerifier() throws StandardException {
        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database_hs256");
        String jwt = createJWT(key);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database_hs256");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT").decodeUsername(jwt));
    }

    @Test(expected = io.jsonwebtoken.ExpiredJwtException.class)
    public void testKeyAccessTokenVerifierExpired() throws StandardException {
        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database_hs256");
        String jwt = createJWT(key,-1);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database_hs256");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT").decodeUsername(jwt));
    }

    @Test(expected = io.jsonwebtoken.security.SignatureException.class)
    public void testKeyAccessTokenVerifierWrongKey() throws StandardException {
        Key key = Keys.secretKeyFor(SignatureAlgorithm.HS256);
        String jwt = createJWT(key,-1);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database_hs256");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT").decodeUsername(jwt));
    }

    @Test
    public void testKeyPairAccessTokenVerifier() throws StandardException {
        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database");
        String jwt = createJWT(key);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT_PUB").decodeUsername(jwt));
    }

    @Test(expected = io.jsonwebtoken.ExpiredJwtException.class)
    public void testKeyPairAccessTokenVerifierExpired() throws StandardException {
        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database");
        String jwt = createJWT(key, -1);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT_PUB").decodeUsername(jwt));
    }

    @Test(expected = io.jsonwebtoken.UnsupportedJwtException.class)
    public void testKeyPairAccessTokenVerifierWrongKey() throws StandardException {
        Key key = Keys.secretKeyFor(SignatureAlgorithm.HS256);
        String jwt = createJWT(key, -1);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, SPLICEMACHINE_COM_ISSUER);
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT_PUB").decodeUsername(jwt));
    }


    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testKeyAccessTokenVerifierWrongIssuer() throws StandardException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Token is from another isser http://splicemachine.com/:http://example.com");

        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database_hs256");
        String jwt = createJWT(key);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database_hs256");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, "http://example.com");
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT").decodeUsername(jwt));
    }

    @Test
    public void testKeyPairAccessTokenWrongIssuer() throws StandardException {
        expectedEx.expect(IllegalArgumentException.class);
        expectedEx.expectMessage("Token is from another isser http://splicemachine.com/:http://example.com");

        Key key = loadKey(KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath(), "admin2020", "splice_database");
        String jwt = createJWT(key);

        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEY_STORE, KeyAccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/splice_database.jks").getPath());
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD, "admin2020");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS, "splice_database");
        System.setProperty(KeyAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, "http://example.com");
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("splice_user", AccessTokenVerifierFactory.createVerifier("SPLICE_JWT_PUB").decodeUsername(jwt));
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
        return  createJWT(key, 5);
    }

    private static String createJWT(Key key, int expiration) {
        Calendar c = Calendar.getInstance();
        c.add(Calendar.SECOND, expiration);  // number of days to add

        Map<String, Object> additionalClaims = new HashMap<>();
        additionalClaims.put("db_user", "splice_user");

        if (key == null) {
            return Jwts.builder().setIssuer(SPLICEMACHINE_COM_ISSUER)
                    .setSubject("splice_user")
                    .addClaims(additionalClaims)
                    .setExpiration(c.getTime())
                    .setAudience("database")
                    .compact();
        }
        return Jwts.builder().setIssuer(SPLICEMACHINE_COM_ISSUER)
                .setSubject("splice_user")
                .setExpiration(c.getTime())
                .setAudience("database")
                .addClaims(additionalClaims)
                .signWith(key).compact();
    }

}
