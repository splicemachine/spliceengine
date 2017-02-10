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

import com.splicemachine.db.client.am.ConnectionCallbackInterface;
import com.splicemachine.db.client.am.SqlException;


public class ConnectionReply {
    private ConnectionReplyInterface materialConnectionReply_;
    com.splicemachine.db.client.am.Agent agent_;

    ConnectionReply(com.splicemachine.db.client.am.Agent agent, ConnectionReplyInterface materialConnectionReply) {
        agent_ = agent;
        materialConnectionReply_ = materialConnectionReply;
    }

    public void readCommitSubstitute(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readCommitSubstitute(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalCommit(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalCommit(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalRollback(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalRollback(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXAStart(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXAStart(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXACommit(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXACommit(connection);
        agent_.checkForChainBreakingException_();
    }

    public void readLocalXARollback(ConnectionCallbackInterface connection) throws SqlException {
        materialConnectionReply_.readLocalXARollback(connection);
        agent_.checkForChainBreakingException_();
    }
}
