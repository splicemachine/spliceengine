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
import io.jsonwebtoken.lang.Assert;

import java.io.*;
import java.security.Key;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

public class Auth0AccessTokenVerifier implements AccessTokenVerifier {

    public static final String SPLICE_JWT_TOKEN_AUTH0_CERT = "splice.jwtToken.auth0.cert";

    private final String issuer;
    private final String audience;
    private final String certificatePath;
    private final String userNameClaim;

    protected Auth0AccessTokenVerifier() {
        issuer = System.getProperty(SPLICE_JWT_TOKEN_ISSUER);
        audience = System.getProperty(SPLICE_JWT_TOKEN_AUDIENCE);
        certificatePath = System.getProperty(SPLICE_JWT_TOKEN_AUTH0_CERT);
        userNameClaim = System.getProperty(SPLICE_JWT_TOKEN_USERNAME);
        Assert.hasText(issuer, "Issuer argument cannot be null or empty.");
        Assert.hasText(audience, "Audience argument cannot be null or empty.");
        Assert.hasText(certificatePath, "Certificate path argument cannot be null or empty.");
        Assert.hasText(userNameClaim, "Audience argument cannot be null or empty.");
    }

    @Override
    public String decodeUsername(String jwtToken) throws StandardException {
        Key key;
        try {
            key = readCertificate(certificatePath);
        } catch (IOException | CertificateException e) {
            throw StandardException.plainWrapException(e);
        }

        Claims jwtBody = Jwts.parserBuilder().setSigningKey(key).build().parseClaimsJws(jwtToken).getBody();

        if (!jwtBody.get("iss").equals(issuer)) {
            throw new IllegalArgumentException(String.format("Token is from another issuer %s:%s", jwtBody.get("iss"), issuer));
        }

        if (!jwtBody.get("aud").equals(audience)) {
            throw new IllegalArgumentException(String.format("Token is for another audience %s:%s", jwtBody.get("aud"), audience));
        }

        return escapeSpecialChars((String) jwtBody.get(userNameClaim));
    }

    private static String escapeSpecialChars(String value) {
        return value.replaceAll("\\||-", "_");
    }

    private static Key readCertificate(String fileName) throws CertificateException, FileNotFoundException {
        CertificateFactory fact = CertificateFactory.getInstance("X.509");
        FileInputStream is = new FileInputStream(fileName);
        X509Certificate cer = (X509Certificate) fact.generateCertificate(is);
        return cer.getPublicKey();
    }

}
