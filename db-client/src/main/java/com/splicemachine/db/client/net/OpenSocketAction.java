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
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
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

import javax.net.SocketFactory;
import javax.net.ssl.SSLSocketFactory;

public class OpenSocketAction implements java.security.PrivilegedExceptionAction {
    private String server_;
    private int port_;
    private int clientSSLMode_;

    public OpenSocketAction(String server, int port, int clientSSLMode) {
        server_ = server;
        port_ = port;
        clientSSLMode_ = clientSSLMode;
    }

    public Object run() 
        throws java.net.UnknownHostException, 
               java.io.IOException,
               java.security.NoSuchAlgorithmException,
               java.security.KeyManagementException,
               java.security.NoSuchProviderException,
               java.security.KeyStoreException,
               java.security.UnrecoverableKeyException,
               java.security.cert.CertificateException
    {
        
        SocketFactory sf;
        switch (clientSSLMode_) {
        case com.splicemachine.db.jdbc.ClientBaseDataSource.SSL_BASIC:
            sf = NaiveTrustManager.getSocketFactory();
            break;
        case com.splicemachine.db.jdbc.ClientBaseDataSource.SSL_PEER_AUTHENTICATION:
            sf = (SocketFactory)SSLSocketFactory.getDefault();
            break;
        case com.splicemachine.db.jdbc.ClientBaseDataSource.SSL_OFF:
            sf = SocketFactory.getDefault();
            break;
        default: 
            // Assumes cleartext for undefined values
            sf = SocketFactory.getDefault();
            break;
        }
        return sf.createSocket(server_, port_);
    }

}
