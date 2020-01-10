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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import  java.sql.*;
import java.util.Arrays;

import com.splicemachine.db.vti.StringColumnVTI;
import com.splicemachine.db.vti.VTICosting;
import com.splicemachine.db.vti.VTIEnvironment;

/**
 * <p>
 * This is a concrete VTI which is prepopulated with rows which are just
 * arrays of string columns.
 * </p>
 */
public    class   StringArrayVTI  extends StringColumnVTI
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  static  final   double  FAKE_ROW_COUNT = 13.0;
    public  static  final   double  FAKE_INSTANTIATION_COST = 3149.0;

    private static  final   String[]    EXPECTED_STACK =
    {
        "deduceGetXXXCaller",
        "getRawColumn",
        "getString",
    };
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // INNER CLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    //
    // Inner classes for testing VTICosting api.
    //
    public  static  class   MissingConstructor  extends StringArrayVTI  implements VTICosting
    {
        private  MissingConstructor( String[] columnNames, String[][] rows ) { super( columnNames, rows ); }

        public  static  ResultSet   dummyVTI()
        {
            return new StringArrayVTI( new String[] { "foo" }, new String[][] { { "bar" } } );
        }
        
        public  double  getEstimatedRowCount( VTIEnvironment env ) throws SQLException
        {
            return FAKE_ROW_COUNT;
        }
        
        public  double  getEstimatedCostPerInstantiation( VTIEnvironment env ) throws SQLException
        {
            return FAKE_INSTANTIATION_COST;
        }
        
        public  boolean supportsMultipleInstantiations( VTIEnvironment env ) throws SQLException
        {
            return false;
        }        
    }
    
    public  static  class   ZeroArgConstructorNotPublic    extends MissingConstructor
    {
        ZeroArgConstructorNotPublic()
        { super( new String[] { "foo" }, new String[][] { { "bar" } } ); }
    }
    
    public  static  class   ConstructorException    extends ZeroArgConstructorNotPublic
    {
        public  ConstructorException()
        {
            super();

            Object      shameOnYou = null;

            // trip over a null pointer exception
            shameOnYou.hashCode();
        }
    }
    
    public  static  class   GoodVTICosting    extends ZeroArgConstructorNotPublic
    {
        public  GoodVTICosting()
        {
            super();
        }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private int             _rowIdx = -1;
    private String[][]      _rows;

    private static  StringBuffer    _callers;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  StringArrayVTI( String[] columnNames, String[][] rows )
    {
        super( columnNames );

        _rows = rows;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * This SQL function returns the list of getXXX() calls made to the last
     * StringArrayVTI.
     * </p>
     */
    public  static  String  getXXXrecord()
    {
        if ( _callers == null ) { return null; }
        else { return _callers.toString(); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ABSTRACT StringColumn BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    protected String  getRawColumn( int columnNumber ) throws SQLException
    {
        String                  callersCallerMethod = deduceGetXXXCaller();

        _callers.append( callersCallerMethod );
        _callers.append( ' ' );

        return  _rows[ _rowIdx ][ columnNumber - 1 ];
    }

    // The stack looks like this:
    //
    // getXXX()
    // getString()
    // getRawColumn()
    // deduceGetXXXCaller()
    //
    // Except if the actual getXXX() method is getString()
    //
    private String  deduceGetXXXCaller() throws SQLException
    {
        StackTraceElement[]     stack = null;
        try {
            stack = (new Throwable()).getStackTrace();
         } catch (Throwable t) { throw new SQLException( t.getMessage() ); }
        
        return locateGetXXXCaller( stack );
   }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public  boolean next() throws SQLException
    {
        if ( (++_rowIdx) >= _rows.length ) { return false; }
        else
        {
            _callers = new StringBuffer();
            return true;
        }
    }

    public  void close() throws SQLException
    {}

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Find the getXXX() method above us on the stack. The stack looks
     * like this:
     * </p>
     *
     * <ul>
     * <li>getXXX()</li>
     * <li>getString()</li>
     * <li>getRawColumn()</li>
     * <li>deduceGetXXXCaller()</li>
     * </ul>
     *
     * </p>    
     * Except if the actual getXXX() method is getString()
     * </p>
     */
    private String  locateGetXXXCaller( StackTraceElement[] stack ) throws SQLException
    {
        String[]        actualMethodNames = squeezeMethodNames( stack );
        String[]        expectedMethodNames = EXPECTED_STACK;
        int             actualIdx = findIndex( "getString", actualMethodNames );

        if ( actualIdx < 0 ) { throw badStack( EXPECTED_STACK, actualMethodNames ); }
       
        String      result = actualMethodNames[ ++actualIdx ];

        if ( !result.startsWith( "get" ) ) { result = "getString"; }

        return result;
    }

    /**
     * <p>
     * Complain that we don't like the stack.
     * </p>
     */
    private SQLException   badStack( String[] expected, String[] actual )
    {
        return new SQLException
            ( "Expected stack to include " + stringify( expected ) + ", but the stack was actually this: " + stringify( actual ) );
    }
    
    /**
     * <p>
     * Look for a  method name on a stack and return its location as an
     * index into the stack. Returns -1 if the expected name is not found.
     * </p>
     */
    private int findIndex( String expectedMethod, String[] actualMethodNames )
    {
        int         count = actualMethodNames.length;
        for ( int i = 0; i < count; i++ )
        {
            if ( expectedMethod.equals( actualMethodNames[ i ] ) ) { return i; }
        }

        return -1;
    }

    /**
     * <p>
     * Extract the names of methods on a stack.
     * </p>
     */
    private String[]    squeezeMethodNames( StackTraceElement[] stack )
    {
        if ( stack == null ) { stack = new StackTraceElement[] {}; }
        int         count = stack.length;
        String[]    result = new String[ count ];

        for ( int i = 0; i < count; i++ )
        {
            result[ i ] = stack[ i ].getMethodName();
        }

        return result;
    }


    /**
     * <p>
     * Turn an array into a printable String.
     * </p>
     */
    private String  stringify( Object[] raw )
    {
        if ( raw == null ) { raw = new Object[] {}; }

        return Arrays.asList( raw ).toString();
    }


}

