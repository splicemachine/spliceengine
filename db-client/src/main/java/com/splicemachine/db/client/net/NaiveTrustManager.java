/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.net;

import java.io.FileInputStream;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;


/**
 * This is a naive trust manager we use when we don't want server
 * authentication. Any certificate will be accepted. 
 **/
public class NaiveTrustManager
    implements X509TrustManager
{

    /**
     * We don't want more than one instence of this TrustManager
     */
    private NaiveTrustManager()
    {
    }

    static private TrustManager[] thisManager = null;

    /** 
     * Generate a socket factory with this trust manager. Derby
     * Utility routine which is not part of the X509TrustManager
     * interface.
     **/
    public static SocketFactory getSocketFactory()
        throws java.security.NoSuchAlgorithmException,
               java.security.KeyManagementException,
               java.security.NoSuchProviderException,
               java.security.KeyStoreException,
               java.security.UnrecoverableKeyException,
               java.security.cert.CertificateException,
               java.io.IOException
    {
        if (thisManager == null) {
            thisManager = new TrustManager [] {new NaiveTrustManager()};
        }

        SSLContext ctx = SSLContext.getInstance("SSL");
        
        if (ctx.getProvider().getName().equals("SunJSSE") &&
            (System.getProperty("javax.net.ssl.keyStore") != null) &&
            (System.getProperty("javax.net.ssl.keyStorePassword") != null)) {
            
            // SunJSSE does not give you a working default keystore
            // when using your own trust manager. Since a keystore is
            // needed on the client when the server does
            // peerAuthentication, we have to provide one working the
            // same way as the default one.

            String keyStore = 
                System.getProperty("javax.net.ssl.keyStore");
            String keyStorePassword =
                System.getProperty("javax.net.ssl.keyStorePassword");
            
            KeyStore ks = KeyStore.getInstance("JKS");
            ks.load(new FileInputStream(keyStore),
                    keyStorePassword.toCharArray());
            
            KeyManagerFactory kmf = 
                KeyManagerFactory.getInstance("SunX509", "SunJSSE");
            kmf.init(ks, keyStorePassword.toCharArray());

            ctx.init(kmf.getKeyManagers(),
                     thisManager,
                     null); // Use default random source
        } else {
            ctx.init(null, // Use default key manager
                     thisManager,
                     null); // Use default random source
        }

        return ctx.getSocketFactory();
     }
    
    /** 
     * Checks wether the we trust the client. Since this trust manager
     * is just for the Derby clients, this routine is actually never
     * called, but need to be here when we implement X509TrustManager.
     * @param chain The client's certificate chain
     * @param authType authorization type (e.g. "RSA" or "DHE_DSS")
     **/
    public void checkClientTrusted(X509Certificate[] chain, 
                                   String authType)
        throws CertificateException
    {
        // Reject all attemtpts to trust a client. We should never end
        // up here.
        throw new CertificateException();
    }
    
    /** 
     * Checks wether the we trust the server, which we allways will.
     * @param chain The server's certificate chain
     * @param authType authorization type (e.g. "RSA" or "DHE_DSS")
     **/
    public void checkServerTrusted(X509Certificate[] chain, 
                                   String authType)
        throws CertificateException
    {
        // Do nothing. We trust everyone.
    }
    
    /**
     * Return an array of certificate authority certificates which are
     * trusted for authenticating peers. Not relevant for this trust
     * manager.
     */
    public X509Certificate[] getAcceptedIssuers()
    {
        return new X509Certificate[0];
    }
    
}
