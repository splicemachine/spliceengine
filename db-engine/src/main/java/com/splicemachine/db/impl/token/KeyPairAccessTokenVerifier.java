package com.splicemachine.db.impl.token;

import com.splicemachine.db.iapi.error.StandardException;

import java.io.FileInputStream;
import java.io.InputStream;
import java.security.Key;
import java.security.KeyStore;
import java.security.cert.Certificate;

public class KeyPairAccessTokenVerifier extends KeyAccessTokenVerifier {

    protected KeyPairAccessTokenVerifier() {
        super();
    }

    protected Key loadKey() throws StandardException {
        try {
            KeyStore keyStore = KeyStore.getInstance("pkcs12");
            try(InputStream keyStoreData = new FileInputStream(getKeyStorePath())){
                keyStore.load(keyStoreData, getKeyStorePassword().toCharArray());
                Certificate cert = keyStore.getCertificate(getKeyStoreAlias());
                return cert.getPublicKey();
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
}
