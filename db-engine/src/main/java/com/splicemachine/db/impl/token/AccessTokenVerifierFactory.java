package com.splicemachine.db.impl.token;

import com.splicemachine.db.authentication.AccessTokenVerifier;

public class AccessTokenVerifierFactory {

    public static AccessTokenVerifier createVerifier(String authenticatorName) {
        TokenAuthenticator authenticator = TokenAuthenticator.valueOf(authenticatorName.toUpperCase());
        switch (authenticator) {
            case OKTA_OAUTH:
                return new OktaAccessTokenVerifier();
            case SPLICE_JWT:
                return new KeyAccessTokenVerifier();
            case SPLICE_JWT_PUB:
                return new KeyPairAccessTokenVerifier();
            default:
                throw new IllegalArgumentException(String.format("%s authenticator is not supported", authenticatorName));
        }
    }
}
