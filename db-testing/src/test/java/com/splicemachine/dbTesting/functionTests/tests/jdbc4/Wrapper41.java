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

package com.splicemachine.dbTesting.functionTests.tests.jdbc4;

import java.sql.SQLException;

import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.client.net.NetResultSet40;
import com.splicemachine.db.impl.jdbc.EmbedCallableStatement40;
import com.splicemachine.db.client.am.CallableStatement40;
import com.splicemachine.db.iapi.jdbc.BrokeredCallableStatement40;
import com.splicemachine.db.client.am.LogicalCallableStatement40;

/**
 * A wrapper around the getObject() overloads added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbedResultSet40    _embedded;
    private NetResultSet40      _netclient;
    private EmbedCallableStatement40 _embedCallableStatement;
    private CallableStatement40 _callableStatement;
    private BrokeredCallableStatement40 _brokeredCallableStatement;
    private LogicalCallableStatement40 _logicalCallableStatement;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EmbedResultSet40 ) { _embedded = (EmbedResultSet40) wrapped; }
        else if ( wrapped instanceof NetResultSet40 ) { _netclient = (NetResultSet40) wrapped; }
        else if ( wrapped instanceof EmbedCallableStatement40 ) { _embedCallableStatement = (EmbedCallableStatement40) wrapped; }
        else if ( wrapped instanceof CallableStatement40 ) { _callableStatement = (CallableStatement40) wrapped; }
        else if ( wrapped instanceof BrokeredCallableStatement40 ) { _brokeredCallableStatement = (BrokeredCallableStatement40) wrapped; }
        else if ( wrapped instanceof LogicalCallableStatement40 ) { _logicalCallableStatement = (LogicalCallableStatement40) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  <T> T getObject( int columnIndex, Class<T> type ) throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getObject( columnIndex, type ); }
        else if ( _netclient != null ) { return _netclient.getObject( columnIndex, type ); }
        else if ( _embedCallableStatement != null ) { return _embedCallableStatement.getObject( columnIndex, type ); }
        else if ( _callableStatement != null ) { return _callableStatement.getObject( columnIndex, type ); }
        else if ( _brokeredCallableStatement != null ) { return _brokeredCallableStatement.getObject( columnIndex, type ); }
        else if ( _logicalCallableStatement != null ) { return _logicalCallableStatement.getObject( columnIndex, type ); }
        else { throw nothingWrapped(); }
    }
    public  <T> T getObject( String columnName, Class<T> type )
        throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getObject( columnName, type ); }
        else if ( _netclient != null ) { return _netclient.getObject( columnName, type ); }
        else if ( _embedCallableStatement != null ) { return _embedCallableStatement.getObject( columnName, type ); }
        else if ( _callableStatement != null ) { return _callableStatement.getObject( columnName, type ); }
        else if ( _brokeredCallableStatement != null ) { return _brokeredCallableStatement.getObject( columnName, type ); }
        else if ( _logicalCallableStatement != null ) { return _logicalCallableStatement.getObject( columnName, type ); }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public Object   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
        else if ( _embedCallableStatement != null ) { return _embedCallableStatement; }
        else if ( _callableStatement != null ) { return _callableStatement; }
        else if ( _brokeredCallableStatement != null ) { return _brokeredCallableStatement; }
        else if ( _logicalCallableStatement != null ) { return _logicalCallableStatement; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

