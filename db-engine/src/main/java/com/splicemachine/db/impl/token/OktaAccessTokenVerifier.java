/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

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
            throw new IllegalArgumentException(String.format("Token is from another issuer %s:%s", jwtBody.get("iss"), issuer));
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
