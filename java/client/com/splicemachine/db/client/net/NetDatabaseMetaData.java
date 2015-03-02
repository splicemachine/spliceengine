/*

   Derby - Class com.splicemachine.db.client.net.NetDatabaseMetaData

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

*/
package com.splicemachine.db.client.net;

import com.splicemachine.db.client.am.Configuration;
import com.splicemachine.db.client.am.ProductLevel;
import com.splicemachine.db.client.am.SqlException;

public class NetDatabaseMetaData extends com.splicemachine.db.client.am.DatabaseMetaData {

    public NetDatabaseMetaData(NetAgent netAgent, NetConnection netConnection) {
        // Consider setting product level during parse
        super(netAgent, netConnection, new ProductLevel(netConnection.productID_,
                netConnection.targetSrvclsnm_,
                netConnection.targetSrvrlslv_));
    }

    //---------------------------call-down methods--------------------------------

    public String getURL_() throws SqlException {
        String urlProtocol;

        urlProtocol = Configuration.jdbcDerbyNETProtocol;

        return
                urlProtocol +
                connection_.serverNameIP_ +
                ":" +
                connection_.portNumber_ +
                "/" +
                connection_.databaseName_;
    }

}
