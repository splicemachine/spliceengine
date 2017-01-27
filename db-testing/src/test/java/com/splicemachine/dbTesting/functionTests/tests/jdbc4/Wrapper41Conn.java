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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

import com.splicemachine.db.impl.jdbc.EmbedConnection40;
import com.splicemachine.db.iapi.jdbc.BrokeredConnection40;
import com.splicemachine.db.client.net.NetConnection40;
import com.splicemachine.db.client.am.LogicalConnection40;

/**
 * A wrapper around the abort(Executor) method added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41Conn
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbedConnection40    _embedded;
    private NetConnection40      _netclient;
    private BrokeredConnection40 _brokeredConnection;
    private LogicalConnection40 _logicalConnection;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41Conn( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EmbedConnection40 ) { _embedded = (EmbedConnection40) wrapped; }
        else if ( wrapped instanceof NetConnection40 ) { _netclient = (NetConnection40) wrapped; }
        else if ( wrapped instanceof BrokeredConnection40 ) { _brokeredConnection = (BrokeredConnection40) wrapped; }
        else if ( wrapped instanceof LogicalConnection40 ) { _logicalConnection = (LogicalConnection40) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  void    abort( Executor executor ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.abort( executor ); }
        else if ( _netclient != null ) { _netclient.abort( executor ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.abort( executor ); }
        else if ( _logicalConnection != null ) { _logicalConnection.abort( executor ); }
        else { throw nothingWrapped(); }
    }

    public  String    getSchema() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getSchema(); }
        else if ( _netclient != null ) { return _netclient.getSchema(); }
        else if ( _brokeredConnection != null ) { return _brokeredConnection.getSchema(); }
        else if ( _logicalConnection != null ) { return _logicalConnection.getSchema(); }
        else { throw nothingWrapped(); }
    }

    public  void    setSchema( String schemaName ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.setSchema( schemaName ); }
        else if ( _netclient != null ) { _netclient.setSchema( schemaName ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.setSchema( schemaName ); }
        else if ( _logicalConnection != null ) { _logicalConnection.setSchema( schemaName ); }
        else { throw nothingWrapped(); }
    }

    public  int    getNetworkTimeout() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getNetworkTimeout(); }
        else if ( _netclient != null ) { return _netclient.getNetworkTimeout(); }
        else if ( _brokeredConnection != null ) { return _brokeredConnection.getNetworkTimeout(); }
        else if ( _logicalConnection != null ) { return _logicalConnection.getNetworkTimeout(); }
        else { throw nothingWrapped(); }
    }

    public  void    setNetworkTimeout( Executor executor, int milliseconds ) throws SQLException
    {
        if ( _embedded != null ) { _embedded.setNetworkTimeout( executor, milliseconds ); }
        else if ( _netclient != null ) { _netclient.setNetworkTimeout( executor, milliseconds ); }
        else if ( _brokeredConnection != null ) { _brokeredConnection.setNetworkTimeout( executor, milliseconds ); }
        else if ( _logicalConnection != null ) { _logicalConnection.setNetworkTimeout( executor, milliseconds ); }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public Connection   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
        else if ( _brokeredConnection != null ) { return _brokeredConnection; }
        else if ( _logicalConnection != null ) { return _logicalConnection; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

