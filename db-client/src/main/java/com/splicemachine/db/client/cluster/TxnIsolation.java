/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.client.am.Connection;
import com.splicemachine.db.iapi.reference.SQLState;

import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 8/19/16
 */
enum TxnIsolation{
    READ_UNCOMMITTED(Connection.TRANSACTION_READ_UNCOMMITTED),
    READ_COMMITTED(Connection.TRANSACTION_READ_COMMITTED),
    REPEATABLE_READ(Connection.TRANSACTION_REPEATABLE_READ),
    SERIALIZABLE(Connection.TRANSACTION_SERIALIZABLE);

    final int level;

    TxnIsolation(int level){
        this.level=level;
    }

    public static TxnIsolation parse(int level) throws SQLException{
        switch(level){
            case Connection.TRANSACTION_READ_UNCOMMITTED: return READ_UNCOMMITTED;
            case Connection.TRANSACTION_READ_COMMITTED: return READ_COMMITTED;
            case Connection.TRANSACTION_REPEATABLE_READ: return REPEATABLE_READ;
            case Connection.TRANSACTION_SERIALIZABLE: return SERIALIZABLE;
            default:
                throw new SQLException("Unknown transaction isolation level: "+level,SQLState.UNIMPLEMENTED_ISOLATION_LEVEL);
        }
    }
}
