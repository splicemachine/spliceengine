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

import java.lang.reflect.Method;
import java.sql.SQLException;

import com.splicemachine.db.iapi.jdbc.EngineStatement;
import com.splicemachine.db.client.am.LogicalPreparedStatement;

/**
 * A wrapper around the new Statement methods added by JDBC 4.1.
 */
public  class   Wrapper41Statement
{
    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private EngineStatement     _engineStatement;
    private com.splicemachine.db.client.am.Statement       _netStatement;
    private LogicalPreparedStatement  _logicalStatement;
    
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////

    public Wrapper41Statement( Object wrapped ) throws Exception
    {
        if ( wrapped instanceof EngineStatement ) { _engineStatement = (EngineStatement) wrapped; }
        else if ( wrapped instanceof com.splicemachine.db.client.am.Statement ) { _netStatement = (com.splicemachine.db.client.am.Statement) wrapped; }
        else if ( wrapped instanceof LogicalPreparedStatement ) { _logicalStatement = (LogicalPreparedStatement) wrapped; }
        else { throw nothingWrapped(); }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // JDBC 4.1 BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public  void    closeOnCompletion() throws SQLException
    {
        if ( _engineStatement != null ) { _engineStatement.closeOnCompletion(); }
        else if ( _netStatement != null ) { _netStatement.closeOnCompletion(); }
        else if ( _logicalStatement != null ) { _logicalStatement.closeOnCompletion(); }
        else { throw nothingWrapped(); }
    }

    public  boolean isCloseOnCompletion() throws SQLException
    {
        if ( _engineStatement != null ) { return _engineStatement.isCloseOnCompletion(); }
        else if ( _netStatement != null ) { return _netStatement.isCloseOnCompletion(); }
        else if ( _logicalStatement != null ) { return _logicalStatement.isCloseOnCompletion(); }
        else { throw nothingWrapped(); }
    }


    ///////////////////////////////////////////////////////////////////////
    //
    // OTHER PUBLIC BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    public java.sql.Statement   getWrappedObject() throws SQLException
    {
        if ( _engineStatement != null ) { return _engineStatement; }
        else if ( _netStatement != null ) { return _netStatement; }
        else if ( _logicalStatement != null ) { return _logicalStatement; }
        else { throw nothingWrapped(); }
    }

    public  boolean isClosed()  throws Exception
    {
        java.sql.Statement  stmt = getWrappedObject();
        Method  method = stmt.getClass().getMethod( "isClosed", null );

        return ((Boolean) method.invoke( stmt, null )).booleanValue();
    }

    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    private SQLException nothingWrapped() { return new SQLException( "Nothing wrapped!" ); }

}

