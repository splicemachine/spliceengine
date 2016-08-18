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

import com.splicemachine.db.iapi.reference.SQLState;
import com.sun.org.apache.xpath.internal.operations.Bool;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 8/18/16
 */
class ClusterConnectionManager{
    private Connection currentConn;
    private final ClusteredDataSource poolSource;

    private final Map<ClusteredStatement,Boolean> openStatementsMap = new IdentityHashMap<>();
    private final Set<ClusteredStatement> openStatementsSet = openStatementsMap.keySet();
    private final Map<ClusteredPreparedStatement,Boolean> openPreparedStatements = new IdentityHashMap<>();

    ClusterConnectionManager(ClusteredDataSource poolSource){
        this.poolSource=poolSource;
    }

    boolean autoCommit = false;

    public void setAutoCommit(boolean autoCommit) throws SQLException{
        if(currentConn!=null){
            if(this.autoCommit!=autoCommit)
                currentConn.setAutoCommit(autoCommit); //force the transaction commit
        }
        this.autoCommit = autoCommit;
    }

    public void commit() throws SQLException{
        if(currentConn!=null){
            currentConn.commit();
            releaseConnectionIfPossible();
        }
    }

    public void rollback() throws SQLException{
        if(currentConn!=null){
            currentConn.rollback();
            releaseConnectionIfPossible();
        }
    }

    public void close() throws SQLException{
        if(openStatementsSet.size()>0||
                openPreparedStatements.size()>0){
            throw new SQLException("There are open statements",SQLState.CANNOT_CLOSE_ACTIVE_CONNECTION);
        }
        if(currentConn!=null)
            currentConn.close(); //this really just returns the connection to the pool, but close enough
    }

    void registerStatement(ClusteredStatement cs){
        openStatementsMap.put(cs,Boolean.TRUE);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void releaseConnectionIfPossible() throws SQLException{
        /*
         * For now, we will only release the connection if there are no outstanding
         * Prepared statements (otherwise we'd have to re-prepare the statement after each
         * commit, which would probably be expensive). On the upside, this means that
         * prepared statements will not re-prepare. On the downside, prepared statements
         * will not distribute their requests across multiple servers unless the previous connection
         * is ended.
         *
         * For now, this works better than attempting to re-prepare for load distribution, but
         * eventually we may need to change that behavior.
         */
        if(openPreparedStatements.size()<=0){
            for(ClusteredStatement s:openStatementsSet){
                s.invalidate();
            }
            currentConn.close(); //closing the connection will return it to the pool
            currentConn=null;
        }
    }
}
