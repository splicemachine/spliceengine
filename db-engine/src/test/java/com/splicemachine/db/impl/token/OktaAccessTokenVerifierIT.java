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
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class OktaAccessTokenVerifierIT {

    @Ignore
    @Test(expected = io.jsonwebtoken.ExpiredJwtException.class)
    public void testOktaAccessTokenVerifier() throws StandardException {
        String oktaToken = "eyJraWQiOiJlR2I0R1o0VGVRejZzV0dtU3otY1NtcFNuR0JtbUlEUlVweVlDMTdkemljIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIwMHUzM2hramZhRVdHQUtpYjVkNiIsIm5hbWUiOiJJZ29yIFByYXpuaWsiLCJlbWFpbCI6InByYXpuaWtpZ29yQHlhaG9vLmNvbSIsInZlciI6MSwiaXNzIjoiaHR0cHM6Ly9kZXYtOTUwMzE4Ni5va3RhLmNvbS9vYXV0aDIvZGVmYXVsdCIsImF1ZCI6IjBvYTMzaHk3YzdmRXlKQUdjNWQ2IiwiaWF0IjoxNjA5NDE0NzQ5LCJleHAiOjE2MDk0MTgzNDksImp0aSI6IklELlJQbjUyRm9UcFUxLTNrQWhfVm9xMldEVXJuVWoxWThEZHBEQVh5THhKcjgiLCJhbXIiOlsicHdkIl0sImlkcCI6IjAwbzMzaGtmbXc4d0Flczg1NWQ2Iiwibm9uY2UiOiJLNHMxTjBqMTRVUktRdUNGeG9OaXRDd3NiZ21iRGZRQjg0M2dYbnBwMEw4IiwicHJlZmVycmVkX3VzZXJuYW1lIjoicHJhem5pa2lnb3JAeWFob28uY29tIiwiYXV0aF90aW1lIjoxNjA5NDA2NjM1LCJhdF9oYXNoIjoiZjJwQWlXSmoxMGxMb2REYTRTSi14dyIsImRiX3VzZXIiOiJpZ29yIn0.T0s6bhAp2LHjzler56jaoogm41rUVqKYGkHgPAG9PoxAlBvD7daU0aTBLT2QvkErWDrsJYLifKPGrgniSfIVjiLmVnJr5Dwj9ku4MRiqJ8i-nlNFC6rTcKogVi0bg02AzBVVP_96XYSRMh9m_xN1Pr1NjUhxCoBHnuSPeB3tN6BCYg-JR_WQfFMe_qpbTeiT1WlxEGYduYTVwHg54aLNyJFVlPKiuv9ptZI5ZtOCSuYez09eUZZ5Wu6sYWnFVMbtcRoW6gCedaVqXj2NJpzod01-dz66aCqENC21LZ626UQej-Qe4CLFNQtsOtinu3yxUDv745JBiQp93k3dhnd0yg\n";

        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_ISSUER, "https://dev-9503186.okta.com/oauth2/default");
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_OKTA_CLIENT_ID, "0oa33hy7c7fEyJAGc5d6");
        System.setProperty(OktaAccessTokenVerifier.SPLICE_JWT_TOKEN_USERNAME, "db_user");

        Assert.assertEquals("igor", AccessTokenVerifierFactory.createVerifier("okta_oauth").decodeUsername(oktaToken));
    }

}
