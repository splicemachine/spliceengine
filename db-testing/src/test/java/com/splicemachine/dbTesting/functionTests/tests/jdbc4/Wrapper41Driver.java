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

import java.sql.Driver;
import java.sql.SQLException;
import java.util.logging.Logger;

import com.splicemachine.db.jdbc.AutoloadedDriver40;
import com.splicemachine.db.jdbc.ClientDriver40;
import com.splicemachine.db.jdbc.Driver40;

/**
 * A wrapper around the methods added by JDBC 4.1.
 * We can eliminate this class after Java 7 goes GA and we are allowed
 * to use the Java 7 compiler to build our released versions of derbyTesting.jar.
 */
public  class   Wrapper41Driver
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private AutoloadedDriver40    _embedded;
    private Driver40            _driver40;
    private ClientDriver40      _netclient;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41Driver( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof AutoloadedDriver40 ) { _embedded = (AutoloadedDriver40) wrapped; }
        else if ( wrapped instanceof Driver40 ) { _driver40 = (Driver40) wrapped; }
        else if ( wrapped instanceof ClientDriver40 ) { _netclient = (ClientDriver40) wrapped; }
        else { throw nothingWrapped( wrapped ); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  Logger    getParentLogger() throws SQLException
    {
        if ( _embedded != null ) { return _embedded.getParentLogger(); }
        else if ( _driver40 != null ) { return _driver40.getParentLogger(); }
        else if ( _netclient != null ) { return _netclient.getParentLogger(); }
        else { throw nothingWrapped( null ); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public Driver   getWrappedObject() throws SQLException
    {
        if ( _embedded != null ) { return _embedded; }
        else if ( _driver40 != null ) { return _driver40; }
        else if ( _netclient != null ) { return _netclient; }
        else { throw nothingWrapped( null ); }
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped( Object wrapped )
    {
        String  wrappedString = (wrapped == null ? "NULL" : wrapped.getClass().getName() );
        return new SQLException( "Nothing wrapped: " + wrappedString );
    }

}

