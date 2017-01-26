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

import com.splicemachine.db.client.am.ClientMessageId;
import com.splicemachine.db.client.am.Configuration;
import com.splicemachine.db.client.am.ProductLevel;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.shared.common.reference.SQLState;

import java.sql.RowIdLifetime;
import java.sql.SQLException;

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

    /**
     * Indicates whether or not this data source supports the SQL
     * <code>ROWID</code> type. Since Derby does not support the
     * <code>ROWID</code> type, return <code>ROWID_UNSUPPORTED</code>.
     *
     * @return <code>ROWID_UNSUPPORTED</code>
     * @exception SQLException if a database access error occurs
     */
    public RowIdLifetime getRowIdLifetime() throws SQLException {
        checkForClosedConnection();
        return RowIdLifetime.ROWID_UNSUPPORTED;
    }

    /**
     * Returns false unless <code>interfaces</code> is implemented
     *
     * @param  interfaces             a Class defining an interface.
     * @return true                   if this implements the interface or
     *                                directly or indirectly wraps an object
     *                                that does.
     * @throws java.sql.SQLException  if an error occurs while determining
     *                                whether this is a wrapper for an object
     *                                with the given interface.
     */
    public boolean isWrapperFor(Class<?> interfaces) throws SQLException {
        return interfaces.isInstance(this);
    }

    /**
     * Returns <code>this</code> if this class implements the interface
     *
     * @param  interfaces a Class defining an interface
     * @return an object that implements the interface
     * @throws java.sql.SQLException if no object if found that implements the
     * interface
     */
    public <T> T unwrap(java.lang.Class<T> interfaces)
            throws SQLException {
        try {
            return interfaces.cast(this);
        } catch (ClassCastException cce) {
            throw new SqlException(null,
                    new ClientMessageId(SQLState.UNABLE_TO_UNWRAP),
                    interfaces).getSQLException();
        }
    }

}
