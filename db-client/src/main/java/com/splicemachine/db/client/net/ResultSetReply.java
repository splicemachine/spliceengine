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

import com.splicemachine.db.client.am.Agent;
import com.splicemachine.db.client.am.ResultSetCallbackInterface;
import com.splicemachine.db.client.am.SqlException;


public class ResultSetReply extends StatementReply {
    private ResultSetReplyInterface materialResultSetReply_;

    public ResultSetReply(Agent agent,
                          ResultSetReplyInterface materialResultSetReply,
                          StatementReplyInterface materialStatementReply,
                          ConnectionReplyInterface materialConnectionReply) {
        super(agent, materialStatementReply, materialConnectionReply);
        materialResultSetReply_ = materialResultSetReply;
    }

    public void readFetch(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }


    // think about splitting out the position cursor stuff from the fetch stuff
    public void readScrollableFetch(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readScrollableFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }

    public void readPositioningFetch(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readPositioningFetch(resultSet);
        agent_.checkForChainBreakingException_();
    }

    public void readCursorClose(ResultSetCallbackInterface resultSet) throws SqlException {
        materialResultSetReply_.readCursorClose(resultSet);
        agent_.checkForChainBreakingException_();
    }
}
