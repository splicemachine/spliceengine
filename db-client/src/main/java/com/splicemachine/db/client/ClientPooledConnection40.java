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

package com.splicemachine.db.client;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.StatementEventListener;
import javax.sql.StatementEvent;
import com.splicemachine.db.jdbc.ClientBaseDataSource;

/**
 *
 * The class extends from the ClientPooledConnection class 
 * and contains implementations for the JDBC 4.0 specific 
 * methods in the javax.sql.PooledConnection interface.
 *
 */

public class ClientPooledConnection40 extends ClientPooledConnection {
    
    /**
     * List of statement event listeners. The list is copied on each write,
     * ensuring that it can be safely iterated over even if other threads or
     * the listeners fired in the same thread add or remove listeners.
     */
    private final CopyOnWriteArrayList<StatementEventListener>
            statementEventListeners =
                    new CopyOnWriteArrayList<StatementEventListener>();

    public ClientPooledConnection40(ClientBaseDataSource ds,
        com.splicemachine.db.client.am.LogWriter logWriter,
        String user,
        String password) throws SQLException {
        super(ds,logWriter,user,password);
        
    }
    
    
    public ClientPooledConnection40(ClientBaseDataSource ds,
        com.splicemachine.db.client.am.LogWriter logWriter,
        String user,
        String password,
        int rmId) throws SQLException {
        super(ds,logWriter,user,password,rmId);
        
    }
    
     /**
     *
     * Registers a StatementEventListener with this PooledConnection object. 
     * Components that wish to be informed of events associated with the 
     * PreparedStatement object created by this PooledConnection like the close 
     * or error occurred event can register a StatementEventListener with this 
     * PooledConnection object.
     *
     * @param  listener A component that implements the StatementEventListener
     *                  interface and wants to be notified of Statement closed or 
     *                  or Statement error occurred events
     */
    public void addStatementEventListener(StatementEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "addStatementEventListener", listener);
        }
        if (listener != null) {
            statementEventListeners.add(listener);
        }
    }
    
    /**
     *
     * Removes the specified previously registered listener object from the list
     * of components that would be informed of events with a PreparedStatement 
     * object.
     * 
     * @param listener The previously registered event listener that needs to be
     *                 removed from the list of components
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        if (logWriter_ != null) {
            logWriter_.traceEntry(this, "removeConnectionEventListener", listener);
        }
        statementEventListeners.remove(listener);
    }
    
    /**
     *
     * Raise the statementClosed event for all the listeners when the 
     * corresponding events occurs.
     *
     * @param statement The PreparedStatement that was closed
     *
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()) {
            StatementEvent event = new StatementEvent(this,statement);
            for (StatementEventListener l : statementEventListeners) {
                l.statementClosed(event);
            }
        }
    }
    
    /**
     *
     * Raise the statementErrorOccurred event for all the listeners when the 
     * corresponding events occurs.
     *
     * @param statement The PreparedStatement on which error occurred
     * @param sqle      The SQLException associated with the error that
     *                  caused the invalidation of the PreparedStatements
     *
     */
    public void onStatementErrorOccurred(PreparedStatement statement,
                                         SQLException sqle) {
        if (!statementEventListeners.isEmpty()) {
            StatementEvent event = new StatementEvent(this,statement,sqle);
            for (StatementEventListener l : statementEventListeners) {
                l.statementErrorOccurred(event);
            }
        }
    }   
}
