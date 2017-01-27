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

import com.splicemachine.db.client.am.ColumnMetaData;
import com.splicemachine.db.client.am.Section;
import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.client.am.Statement;
import com.splicemachine.db.jdbc.ClientDriver;

public class NetStatement implements com.splicemachine.db.client.am.MaterialStatement {

    Statement statement_;


    // Alias for (NetConnection) statement_.connection
    NetConnection netConnection_;

    // Alias for (NetAgent) statement_.agent
    NetAgent netAgent_;


    // If qryrowset is sent on opnqry then it also needs to be sent on every subsequent cntqry.
    public boolean qryrowsetSentOnOpnqry_ = false;

    //---------------------constructors/finalizer---------------------------------

    private NetStatement() {
        initNetStatement();
    }

    private void resetNetStatement() {
        initNetStatement();
    }

    private void initNetStatement() {
        qryrowsetSentOnOpnqry_ = false;
    }

    // Relay constructor for NetPreparedStatement.
    NetStatement(com.splicemachine.db.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        this();
        initNetStatement(statement, netAgent, netConnection);
    }

    void resetNetStatement(com.splicemachine.db.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        resetNetStatement();
        initNetStatement(statement, netAgent, netConnection);
    }

    private void initNetStatement(com.splicemachine.db.client.am.Statement statement, NetAgent netAgent, NetConnection netConnection) {
        netAgent_ = netAgent;
        netConnection_ = netConnection;
        statement_ = statement;
        statement_.materialStatement_ = this;
    }

    // Called by abstract Connection.createStatement().newStatement() for jdbc 1 statements
    NetStatement(NetAgent netAgent, NetConnection netConnection) throws SqlException {
        this(ClientDriver.getFactory().newStatement(netAgent, netConnection),
                netAgent,
                netConnection);
    }

    void netReset(NetAgent netAgent, NetConnection netConnection) throws SqlException {
        statement_.resetStatement(netAgent, netConnection);
        resetNetStatement(statement_, netAgent, netConnection);
    }

    public void reset_() {
        qryrowsetSentOnOpnqry_ = false;
    }

    // Called by abstract Connection.createStatement().newStatement() for jdbc 2 statements with scroll attributes
    NetStatement(NetAgent netAgent, NetConnection netConnection, int type, int concurrency, int holdability) throws SqlException {
        this(ClientDriver.getFactory().newStatement(netAgent, netConnection, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, 
                null,null
                ),
                netAgent,
                netConnection);
    }

    void resetNetStatement(NetAgent netAgent, NetConnection netConnection, int type, int concurrency, int holdability) throws SqlException {
        statement_.resetStatement(netAgent, netConnection, type, concurrency, holdability, java.sql.Statement.NO_GENERATED_KEYS, 
                null, null);
        resetNetStatement(statement_, netAgent, netConnection);
    }

    // ------------------------abstract box car methods-----------------------------------------------

    public void writeSetSpecialRegister_(Section section, java.util.ArrayList sqlsttList) throws SqlException {
        netAgent_.statementRequest_.writeSetSpecialRegister(section,sqlsttList);
    }

    public void readSetSpecialRegister_() throws SqlException {
        netAgent_.statementReply_.readSetSpecialRegister(statement_);
    }

    public void writeExecuteImmediate_(String sql,
                                       Section section) throws SqlException {
        netAgent_.statementRequest_.writeExecuteImmediate(this, sql, section);
    }

    public void readExecuteImmediate_() throws SqlException {
        netAgent_.statementReply_.readExecuteImmediate(statement_);
    }

    // NOTE: NET processing does not require parameters supplied on the "read-side" so parameter sql is ignored.
    public void readExecuteImmediateForBatch_(String sql) throws SqlException {
        readExecuteImmediate_();
    }

    public void writePrepareDescribeOutput_(String sql,
                                            Section section) throws SqlException {
        netAgent_.statementRequest_.writePrepareDescribeOutput(this, sql, section);
    }

    public void readPrepareDescribeOutput_() throws SqlException {
        netAgent_.statementReply_.readPrepareDescribeOutput(statement_);
    }

    public void writeOpenQuery_(Section section,
                                int fetchSize,
                                int resultSetType)
            throws SqlException {
        netAgent_.statementRequest_.writeOpenQuery(this,
                section,
                fetchSize,
                resultSetType);
    }

    public void readOpenQuery_() throws SqlException {
        netAgent_.statementReply_.readOpenQuery(statement_);
    }

    public void writeExecuteCall_(boolean outputExpected,
                                  String procedureName,
                                  Section section,
                                  int fetchSize,
                                  boolean suppressResultSets,
                                  int resultSetType,
                                  ColumnMetaData parameterMetaData,
                                  Object[] inputs) throws SqlException {
        netAgent_.statementRequest_.writeExecuteCall(this,
                outputExpected,
                procedureName,
                section,
                fetchSize,
                suppressResultSets,
                resultSetType,
                parameterMetaData,
                inputs);
    }

    public void readExecuteCall_() throws SqlException {
        netAgent_.statementReply_.readExecuteCall(statement_);
    }

    public void writePrepare_(String sql, Section section) throws SqlException {
        netAgent_.statementRequest_.writePrepare(this, sql, section);
    }

    public void readPrepare_() throws SqlException {
        netAgent_.statementReply_.readPrepare(statement_);
    }

    public void markClosedOnServer_() {
    }
}
