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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.SQLException;

import com.splicemachine.db.client.am.ClientDatabaseMetaData;
import com.splicemachine.db.impl.jdbc.EmbedDatabaseMetaData;

/**
 * A wrapper around the new DatabaseMetaData methods added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41DBMD
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbedDatabaseMetaData    _embedded;
    private ClientDatabaseMetaData _netclient;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41DBMD( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EmbedDatabaseMetaData ) { _embedded = (EmbedDatabaseMetaData) wrapped; }
        else if ( wrapped instanceof ClientDatabaseMetaData) { _netclient = (ClientDatabaseMetaData) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  boolean    generatedKeyAlwaysReturned() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.generatedKeyAlwaysReturned(); }
        else if ( _netclient != null ) { return _netclient.generatedKeyAlwaysReturned(); }
        else { throw nothingWrapped(); }
    }

    public  java.sql.ResultSet    getPseudoColumns
        ( String catalog, String schemaPattern, String tableNamePattern, String columnNamePattern )
        throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getPseudoColumns( catalog, schemaPattern, tableNamePattern, columnNamePattern ); }
        else if ( _netclient != null ) { return _netclient.getPseudoColumns( catalog, schemaPattern, tableNamePattern, columnNamePattern ); }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public java.sql.DatabaseMetaData   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

