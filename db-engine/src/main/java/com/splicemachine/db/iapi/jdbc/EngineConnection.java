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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLWarning;


/**
 * Additional methods the embedded engine exposes on its Connection object
 * implementations. An internal api only, mainly for the network
 * server. Allows consistent interaction between EmbedConnections
 * and BrokeredConnections.
 * 
 */
public interface EngineConnection extends Connection {

    /**
     * Set the DRDA identifier for this connection.
     */
    void setDrdaID(String drdaID);

    /**
     * Is this a global transaction
     * @return true if this is a global XA transaction
     */
    boolean isInGlobalTransaction();
    
    /** 
     * Set the transaction isolation level that will be used for the 
     * next prepare.  Used by network server to implement DB2 style 
     * isolation levels.
     * Note the passed in level using the Derby constants from
     * ExecutionContext and not the JDBC constants from java.sql.Connection.
     * @param level Isolation level to change to.  level is the DB2 level
     *               specified in the package names which happen to correspond
     *               to our internal levels. If 
     *               level == ExecutionContext.UNSPECIFIED_ISOLATION,
     *               the statement won't be prepared with an isolation level.
     * 
     * 
     */
    void setPrepareIsolation(int level) throws SQLException;

    /**
     * Return prepare isolation 
     */
    int getPrepareIsolation()
        throws SQLException;

    /**
     * Get the holdability of the connection. 
     * Identical to JDBC 3.0 method, to allow holdabilty
     * to be supported in JDK 1.3 by the network server,
     * e.g. when the client is jdk 1.4 or above.
     * Can be removed once JDK 1.3 is no longer supported.
     */
    int getHoldability() throws SQLException;
    
    /**
     * Add a SQLWarning to this Connection object.
     * @param newWarning Warning to be added, will be chained to any
     * existing warnings.
     */
    void addWarning(SQLWarning newWarning)
        throws SQLException;

    /**
    * Clear the HashTable of all entries.
    * Called when a commit or rollback of the transaction
    * happens.
    */
    void clearLOBMapping() throws SQLException;

    /**
    * Get the LOB reference corresponding to the locator.
    * @param key the integer that represents the LOB locator value.
    * @return the LOB Object corresponding to this locator.
    */
    Object getLOBMapping(int key) throws SQLException;

    /**
     * Obtain the name of the current schema, so that the NetworkServer can
     * use it for piggy-backing
     * @return the current schema name
     * @throws java.sql.SQLException
     */
    String getCurrentSchemaName() throws SQLException;

    /**
     * Resets the connection before it is returned from a PooledConnection
     * to a new application request (wrapped by a BrokeredConnection).
     * <p>
     * Note that resetting the transaction isolation level is not performed as
     * part of this method. Temporary tables, IDENTITY_VAL_LOCAL and current
     * schema are reset.
     */
    void resetFromPool() throws SQLException;

    /**
     * Return an exception factory that could be used to generate
     * {@code SQLException}s raised by this connection.
     *
     * @return an exception factory instance
     */
    ExceptionFactory getExceptionFactory();

    ////////////////////////////////////////////////////////////////////
    //
    // INTRODUCED BY JDBC 4.1 IN JAVA 7
    //
    ////////////////////////////////////////////////////////////////////

    /**
     * Get the name of the current schema.
     */
    String   getSchema() throws SQLException;

    /**
     * Set the default schema for the Connection.
     */
    void   setSchema(String schemaName) throws SQLException;
    
}
