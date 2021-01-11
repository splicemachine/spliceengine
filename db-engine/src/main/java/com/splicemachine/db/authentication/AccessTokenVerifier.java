package com.splicemachine.db.authentication;

import com.splicemachine.db.iapi.error.StandardException;

public interface AccessTokenVerifier {
    String SPLICE_JWT_TOKEN_USERNAME = "splice.jwtToken.username";
    String SPLICE_JWT_TOKEN_ISSUER = "splice.jwtToken.issuer";

    String decodeUsername(String jwtToken) throws StandardException;

}
