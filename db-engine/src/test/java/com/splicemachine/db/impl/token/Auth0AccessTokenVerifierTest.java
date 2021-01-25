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

public class Auth0AccessTokenVerifierTest {

    public static final String ISSUER = "https://splicemachine-dev.auth0.com/";

    private static final String EXPIRED_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EWTFOVFl5UVRGRFEwRkdNa0ZFTWtFMU1qVTRORVE1T0RJNU1VUTBORFJETlRFeFFUaEdSQSJ9.eyJpc3MiOiJodHRwczovL3NwbGljZW1hY2hpbmUtZGV2LmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNjUzMjM4OTU1Mzk4Mzk1NzQ1NyIsImF1ZCI6Im4xMExtS3RvaldURmtHSkNJQVF6RllzSjlBWW1yNW9oIiwiaWF0IjoxNjExNTY5NTAwLCJleHAiOjE2MTE1Njk1NjB9.nYoPRmw63brpipxmRyrkJrAnthvvWaSECJlkgz2dbcWeZC7k2kkVFXQCo1G405oMWCswGLU_UgpOxhYqxGcKMcRuFcTMR1hFd5TH4czvFRmGK22gxcP4gr8yPWpXkwolisewibDdD9bZCcsO1QD1vDZ65VA7KKMMVXSOzwZ_JEHrtWkL7JDXAreP2VJjREpkDV9c8KRoqMw1-Fazb3_9xuXaAJXSTUFojty8tx8hGX5HUF3bP9ODemQBCgzzGPkz7h_RF7cnrG3V_cm3QFiLzafE5ehBjiXYIVvjr04efKD5ynfvyOb9EBn729m_1gJ4wX85V-_tviiIt2iLlFAM4g";

    private static final String EXPIRE_IN_2031_TOKEN = "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCIsImtpZCI6Ik1EWTFOVFl5UVRGRFEwRkdNa0ZFTWtFMU1qVTRORVE1T0RJNU1VUTBORFJETlRFeFFUaEdSQSJ9.eyJpc3MiOiJodHRwczovL3NwbGljZW1hY2hpbmUtZGV2LmF1dGgwLmNvbS8iLCJzdWIiOiJnb29nbGUtb2F1dGgyfDEwNjUzMjM4OTU1Mzk4Mzk1NzQ1NyIsImF1ZCI6Im4xMExtS3RvaldURmtHSkNJQVF6RllzSjlBWW1yNW9oIiwiaWF0IjoxNjExNTY5ODU4LCJleHAiOjE5MjY5Mjk4NTh9.GUzOpOHYXT8Bkelb-4KUwSUlQfUU9-3o5YPAjEBgMoElkkfm5K2TzrQ5Aib9OycQTW_hfCZUn7CTqhNdFe2Y57ybfc1vx6-1LvHeAFtCqa7z9MKfS_VtkUJTni8FRtCnqFxQg7caOrtOZoFTf14KLvVj3SlqutJ3D2-9k6Horj848G7kZUEmsqWT5K6-4_oAlJth3vMG1ynJ0gG39QsIaS84bOtXKoSMgC8oxU36HHTNmL76qyMdZfYmDgcakM8A6-8riPVYg8O99nb921BF6u5NjEgnd8wbzXoQmKdSx3obYwKPmCdDe-PH0x51jHhk0rHPMo1GNpw0JA_Lr-EdWw";

    private static final String AUDIENCE = "n10LmKtojWTFkGJCIAQzFYsJ9AYmr5oh";

    @Test
    public void testTokenVerifier() throws StandardException {
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUTH0_CERT, Auth0AccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/auth0_sm.pub").getPath());
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, ISSUER);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUDIENCE, AUDIENCE);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "sub");

        Assert.assertEquals("google_oauth2_106532389553983957457", AccessTokenVerifierFactory.createVerifier("SPLICE_OAUTH").decodeUsername(EXPIRE_IN_2031_TOKEN));
    }

    @Test(expected = io.jsonwebtoken.ExpiredJwtException.class)
    public void testTokenExpired() throws StandardException {
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUTH0_CERT, Auth0AccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/auth0_sm.pub").getPath());
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, ISSUER);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUDIENCE, AUDIENCE);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "sub");

        Assert.assertEquals("google_oauth2_106532389553983957457", AccessTokenVerifierFactory.createVerifier("SPLICE_OAUTH").decodeUsername(EXPIRED_TOKEN));
    }

    @Rule
    public ExpectedException expectedEx = ExpectedException.none();

    @Test
    public void testWrongCert() throws StandardException {
        expectedEx.expect(io.jsonwebtoken.security.SignatureException.class);
        expectedEx.expectMessage("JWT signature does not match locally computed signature. JWT validity cannot be asserted and should not be trusted.");
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUTH0_CERT, Auth0AccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/auth0_y.pub").getPath());
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, ISSUER);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUDIENCE, AUDIENCE);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "sub");

        Assert.assertEquals("google_oauth2_106532389553983957457", AccessTokenVerifierFactory.createVerifier("SPLICE_OAUTH").decodeUsername(EXPIRE_IN_2031_TOKEN));
    }

    @Test
    public void testWrongIssuer() throws StandardException {
        expectedEx.expect(java.lang.IllegalArgumentException.class);
        expectedEx.expectMessage("Token is from another issuer https://splicemachine-dev.auth0.com/:https://splicemachine-dev.auth0.com/test");
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUTH0_CERT, Auth0AccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/auth0_sm.pub").getPath());
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, ISSUER + "test");
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUDIENCE, AUDIENCE);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "sub");

        Assert.assertEquals("google_oauth2_106532389553983957457", AccessTokenVerifierFactory.createVerifier("SPLICE_OAUTH").decodeUsername(EXPIRE_IN_2031_TOKEN));
    }

    @Test
    public void testWrongAudience() throws StandardException {
        expectedEx.expect(java.lang.IllegalArgumentException.class);
        expectedEx.expectMessage("Token is for another audience n10LmKtojWTFkGJCIAQzFYsJ9AYmr5oh:n10LmKtojWTFkGJCIAQzFYsJ9AYmr5ohtest");
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUTH0_CERT, Auth0AccessTokenVerifierTest.class.getResource("/com.splicemachine.db.impl.token/auth0_sm.pub").getPath());
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, ISSUER);
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_AUDIENCE, AUDIENCE + "test");
        System.setProperty(Auth0AccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "sub");

        Assert.assertEquals("google_oauth2_106532389553983957457", AccessTokenVerifierFactory.createVerifier("SPLICE_OAUTH").decodeUsername(EXPIRE_IN_2031_TOKEN));
    }

}
