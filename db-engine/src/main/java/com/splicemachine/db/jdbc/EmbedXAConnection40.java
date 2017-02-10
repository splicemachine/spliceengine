/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */


package com.splicemachine.db.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.sql.StatementEvent;
import javax.sql.StatementEventListener;
import javax.sql.XAConnection;
import com.splicemachine.db.iapi.jdbc.ResourceAdapter;

/**
 * This class implements jdbc4.0 methods of XAConnection
 */
final class EmbedXAConnection40 extends EmbedXAConnection
        implements XAConnection {
    
    /**
     * List of statement event listeners. The list is copied on each write,
     * ensuring that it can be safely iterated over even if other threads or
     * the listeners fired in the same thread add or remove listeners.
     */
    private final CopyOnWriteArrayList<StatementEventListener>
            statementEventListeners =
                    new CopyOnWriteArrayList<StatementEventListener>();
    
    /**
     * Creates EmbedXAConnection40.
     * @param ds 
     * @param ra 
     * @param user 
     * @param password 
     * @param requestPassword 
     */
    	EmbedXAConnection40 (EmbeddedDataSource ds, ResourceAdapter ra, 
                String user, String password, 
                boolean requestPassword) throws SQLException {
		super(ds, ra, user, password, requestPassword);
	}
    /**
     * Removes the specified <code>StatementEventListener</code> from the list of 
     * components that will be notified when the driver detects that a 
     * <code>PreparedStatement</code> has been closed or is invalid.
     * <p> 
     * 
     * @param listener	the component which implements the
     * <code>StatementEventListener</code> interface that was previously 
     * registered with this <code>PooledConnection</code> object
     * <p>
     * @since 1.6
     */
    public void removeStatementEventListener(StatementEventListener listener) {
        if (listener == null)
            return;
        statementEventListeners.remove(listener);
    }

    /**
     * Registers a <code>StatementEventListener</code> with this 
     * <code>PooledConnection</code> object.  Components that 
     * wish to be notified when  <code>PreparedStatement</code>s created by the
     * connection are closed or are detected to be invalid may use this method 
     * to register a <code>StatementEventListener</code> with this 
     * <code>PooledConnection</code> object.
     * <p>
     * 
     * @param listener	an component which implements the 
     * <code>StatementEventListener</code> interface that is to be registered
     * with this <code>PooledConnection</code> object
     * <p>
     * @since 1.6
     */
    public void addStatementEventListener(StatementEventListener listener) {
         if (!isActive)
            return;
        if (listener == null)
            return;
        statementEventListeners.add(listener);
    }
    
    /**
     * Raise the statementClosed event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     */
    public void onStatementClose(PreparedStatement statement) {
        if (!statementEventListeners.isEmpty()){
            StatementEvent event = new StatementEvent(this,statement);
            for (StatementEventListener l : statementEventListeners) {
                l.statementClosed(event);
            }
        }
    }
    
    /**
     * Raise the statementErrorOccurred event for all the listeners when the
     * corresponding events occurs
     * @param statement PreparedStatement
     * @param sqle      SQLException
     */
    public void onStatementErrorOccurred(PreparedStatement statement,SQLException sqle) {
        if (!statementEventListeners.isEmpty()){
            StatementEvent event = new StatementEvent(this,statement,sqle);
            for (StatementEventListener l : statementEventListeners) {
                l.statementErrorOccurred(event);
            }
        }
    }
   
}