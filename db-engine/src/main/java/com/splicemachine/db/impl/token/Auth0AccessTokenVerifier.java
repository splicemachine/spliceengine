package com.splicemachine.db.impl.token;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.splicemachine.db.authentication.AccessTokenVerifier;
import com.splicemachine.db.iapi.error.StandardException;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.lang.Assert;

import java.io.*;
import java.math.BigInteger;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.RSAPublicKeySpec;
import java.util.Base64;
import io.jsonwebtoken.io.Decoders;
import io.jsonwebtoken.lang.Strings;

public class OktaAccessTokenVerifier implements AccessTokenVerifier {

    public static final String SPLICE_JWT_TOKEN_OKTA_CLIENT_ID = "splice.jwtToken.okta.clientId";

    private final String issuer;
    private final String audience;
    private final String userNameClaim;

    protected OktaAccessTokenVerifier() {
        issuer = System.getProperty(SPLICE_JWT_TOKEN_ISSUER);
        audience = System.getProperty(SPLICE_JWT_TOKEN_OKTA_CLIENT_ID);
        userNameClaim = System.getProperty(SPLICE_JWT_TOKEN_USERNAME);
        Assert.hasText(issuer, "Issuer argument cannot be null or empty.");
        Assert.hasText(audience, "Audience argument cannot be null or empty.");
        Assert.hasText(userNameClaim, "Audience argument cannot be null or empty.");
    }

    @Override
    public String decodeUsername(String jwtToken) throws StandardException {
        Key key;
        try {
            String headerJson = new String(Decoders.BASE64.decode(jwtToken.split("\\.")[0]), Strings.UTF_8);
            String keyId = new ObjectMapper().readTree(headerJson).get("kid").asText();
            key = readOAuthPubKey(issuer + "/v1/keys", keyId);
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }

        Claims jwtBody = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(jwtToken).getBody();

        if (!jwtBody.get("iss").equals(issuer)) {
            throw new IllegalArgumentException(String.format("Token is from another isser %s:%s", jwtBody.get("iss"), issuer));
        }

        if (!jwtBody.get("aud").equals(audience)) {
            throw new IllegalArgumentException(String.format("Token is for another audience %s:%s", jwtBody.get("aud"), audience));
        }

        return (String) jwtBody.get(userNameClaim);
    }

    private static Key readOAuthPubKey(String url, String keyId) throws IOException {
        JsonNode parent;

        try (InputStream is = new URL(url).openStream()){
            BufferedReader rd = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String jsonText = readAll(rd);
            parent = new ObjectMapper().readTree(jsonText);
        }

        ArrayNode keyNodes = (ArrayNode) parent.get("keys");
        for (int i = 0; i < keyNodes.size(); i++) {
            ObjectNode keyNode = (ObjectNode) keyNodes.get(i);
            if (keyNode.get("kty").asText().equals("RSA") && keyNode.get("use").asText().equals("sig")) {
                if (!keyNode.get("kid").asText().equals(keyId)) {
                    continue;
                }
                RSAPublicKeySpec rsaPublicKeySpec = new RSAPublicKeySpec(base64ToBigInteger(keyNode.get("n").asText()), base64ToBigInteger(keyNode.get("e").asText()));
                try {
                    KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                    return keyFactory.generatePublic(rsaPublicKeySpec);
                } catch (NoSuchAlgorithmException | InvalidKeySpecException e) {
                    throw new IllegalStateException("Failed to parse public key");
                }
            }
        }
        throw new IOException("Issuer Public Key is not found");
    }

    private static String readAll(Reader rd) throws IOException {
        StringBuilder sb = new StringBuilder();
        int cp;
        while ((cp = rd.read()) != -1) {
            sb.append((char) cp);
        }
        return sb.toString();
    }

    private static BigInteger base64ToBigInteger(String value) {
        return new BigInteger(1, Base64.getUrlDecoder().decode(value));
    }

}
