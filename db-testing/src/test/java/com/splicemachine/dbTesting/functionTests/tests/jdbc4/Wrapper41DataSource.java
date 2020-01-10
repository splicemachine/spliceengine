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
import java.util.logging.Logger;
import javax.sql.CommonDataSource;

import com.splicemachine.db.jdbc.EmbeddedDataSource40;
import com.splicemachine.db.jdbc.EmbeddedConnectionPoolDataSource40;
import com.splicemachine.db.jdbc.EmbeddedXADataSource40;
import com.splicemachine.db.jdbc.ClientDataSource40;
import com.splicemachine.db.jdbc.ClientConnectionPoolDataSource40;
import com.splicemachine.db.jdbc.ClientXADataSource40;

/**
 * A wrapper around the methods added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41DataSource
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EmbeddedDataSource40    _embedded;
    private ClientDataSource40      _netclient;
    private EmbeddedConnectionPoolDataSource40    _ecpds;
    private EmbeddedXADataSource40    _exads;
    private ClientConnectionPoolDataSource40      _ccpds;
    private ClientXADataSource40      _cxads;

    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41DataSource( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EmbeddedDataSource40  ) { _embedded = (EmbeddedDataSource40 ) wrapped; }
        else if ( wrapped instanceof ClientDataSource40 ) { _netclient = (ClientDataSource40) wrapped; }
        else if ( wrapped instanceof EmbeddedConnectionPoolDataSource40 ) { _ecpds = (EmbeddedConnectionPoolDataSource40) wrapped; }
        else if ( wrapped instanceof EmbeddedXADataSource40 ) { _exads = (EmbeddedXADataSource40) wrapped; }
        else if ( wrapped instanceof ClientConnectionPoolDataSource40 ) { _ccpds = (ClientConnectionPoolDataSource40) wrapped; }
        else if ( wrapped instanceof ClientXADataSource40 ) { _cxads = (ClientXADataSource40) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  Logger    getParentLogger() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getParentLogger(); }
        else if ( _netclient != null ) { return _netclient.getParentLogger(); }
        else if ( _ecpds != null ) { return _ecpds.getParentLogger(); }
        else if ( _exads != null ) { return _exads.getParentLogger(); }
        else if ( _ccpds != null ) { return _ccpds.getParentLogger(); }
        else if ( _cxads != null ) { return _cxads.getParentLogger(); }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public CommonDataSource   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _netclient != null ) { return _netclient; }
        else if ( _ecpds != null ) { return _ecpds; }
        else if ( _exads != null ) { return _exads; }
        else if ( _ccpds != null ) { return _ccpds; }
        else if ( _cxads != null ) { return _cxads; }
        else { throw nothingWrapped(); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

