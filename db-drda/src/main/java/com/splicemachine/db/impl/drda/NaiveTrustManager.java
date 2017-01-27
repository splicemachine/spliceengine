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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.drda;

import java.io.FileInputStream;
import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.net.ssl.KeyManagerFactory;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.security.cert.CertificateException;
import com.splicemachine.db.iapi.services.property.PropertyUtil;


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
            (PropertyUtil.getSystemProperty("javax.net.ssl.keyStore") != null) &&
            (PropertyUtil.getSystemProperty("javax.net.ssl.keyStorePassword") != null)) {
            
            // SunJSSE does not give you a working default keystore
            // when using your own trust manager. Since a keystore is
            // needed on the client when the server does
            // peerAuthentication, we have to provide one working the
            // same way as the default one.

            String keyStore = 
                PropertyUtil.getSystemProperty("javax.net.ssl.keyStore");
            String keyStorePassword =
                PropertyUtil.getSystemProperty("javax.net.ssl.keyStorePassword");
            
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
