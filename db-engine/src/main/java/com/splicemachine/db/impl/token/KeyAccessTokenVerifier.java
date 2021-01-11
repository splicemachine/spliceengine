package com.splicemachine.db.impl.token;

import com.splicemachine.db.authentication.AccessTokenVerifier;
import com.splicemachine.db.iapi.error.StandardException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.apache.commons.lang3.StringUtils;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;

public class KeyAccessTokenVerifier implements AccessTokenVerifier {

    public static final String SPLICE_JWT_TOKEN_JKS_KEY_STORE = "splice.jwtToken.jksKeyStore";
    public static final String SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD = "splice.jwtToken.jksKeystore.password";
    public static final String SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS = "splice.jwtToken.jksKeystore.alias";

    private final String keyStorePath;
    private final String keyStorePassword;
    private final String keyStoreAlias;
    private final String issuer;

    /**
     *                 splice.jwtToken.jksKeyStore
     *                 splice.jwtToken.jksKeystore.password
     *                 splice.jwtToken.jksKeystore.alias
     */
    protected KeyAccessTokenVerifier() {
        keyStorePath = System.getProperty(SPLICE_JWT_TOKEN_JKS_KEY_STORE);
        keyStorePassword = System.getProperty(SPLICE_JWT_TOKEN_JKS_KEYSTORE_PASSWORD);
        keyStoreAlias = System.getProperty(SPLICE_JWT_TOKEN_JKS_KEYSTORE_ALIAS);
        issuer = System.getProperty(SPLICE_JWT_TOKEN_ISSUER);
        assert StringUtils.isNotEmpty(keyStorePath);
        assert StringUtils.isNotEmpty(keyStorePassword);
        assert StringUtils.isNotEmpty(keyStoreAlias);
        assert StringUtils.isNotEmpty(issuer);
    }

    @Override
    public String decodeUsername(String jwtToken) throws StandardException {
        Key key = loadKey();
        String userNameClaim = System.getProperty(AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME);
        assert StringUtils.isNotEmpty(userNameClaim);

        Claims jwtBody = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(jwtToken).getBody();

        if (!jwtBody.get("iss").equals(issuer)) {
            throw new IllegalArgumentException(String.format("Token is from another isser %s:%s", jwtBody.get("iss"), issuer));
        }

        return (String) jwtBody.get(userNameClaim);
    }

    protected Key loadKey() throws StandardException {
        try {
            KeyStore keyStore = KeyStore.getInstance("pkcs12");
            try(InputStream keyStoreData = new FileInputStream(keyStorePath)){
                keyStore.load(keyStoreData, keyStorePassword.toCharArray());
                return keyStore.getKey(keyStoreAlias, keyStorePassword.toCharArray());
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    protected String getKeyStorePath() {
        return keyStorePath;
    }

    protected String getKeyStorePassword() {
        return keyStorePassword;
    }

    protected String getKeyStoreAlias() {
        return keyStoreAlias;
    }

}
