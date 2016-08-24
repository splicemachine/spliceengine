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

import java.sql.*;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
class ClusteredStatement extends ErrorTrappingStatement{
    private final RefCountedConnection referenceConn;
    private final ClusteredConnection sourceConnection;

    private final Statement statement;

    public ClusteredStatement(RefCountedConnection referenceConn,
                              ClusteredConnection sourceConnection,
                              Statement statement){
        super(statement);
        this.referenceConn=referenceConn;
        this.sourceConnection=sourceConnection;
        this.statement=statement;
    }

    @Override
    public void close() throws SQLException{
        statement.close();
        referenceConn.release();
        sourceConnection.statementClosed(this);
    }

    @Override
    public Connection getConnection() throws SQLException{
        return sourceConnection;
    }

    @Override
    protected void reportError(Throwable t){
        //TODO -sf- consider automatically getting a new statement if autoCommit is off

    }
}
