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

package com.splicemachine.db.client.am;

import com.splicemachine.db.shared.common.reference.SQLState;

import java.sql.SQLException;

public class Savepoint implements java.sql.Savepoint {
    // ----------------- internals -----------------------------------------------

    int savepointId_ = 0;
    String savepointName_ = null;
    Agent agent_;

    //---------------------constructors/finalizer---------------------------------

    // create a named savepoint.
    Savepoint(Agent agent, String savepointName) {
        agent_ = agent;
        savepointName_ = savepointName;
    }

    // create an un-named savepoint.
    Savepoint(Agent agent, int savepointId) {
        agent_ = agent;
        savepointId_ = savepointId;
    }

    // ----------------- externals -----------------------------------------------

    public int getSavepointId() throws SQLException {
        if (savepointId_ != 0) {
            return savepointId_;
        } else {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_ID_FOR_NAMED_SAVEPOINT)).getSQLException();
        }
    }

    public String getSavepointName() throws SQLException {
        if (savepointName_ != null) {
            return savepointName_;
        } else {
            throw new SqlException(agent_.logWriter_, 
                new ClientMessageId(SQLState.NO_NAME_FOR_UNNAMED_SAVEPOINT)).getSQLException();
        }
    }
}
