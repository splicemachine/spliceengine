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
