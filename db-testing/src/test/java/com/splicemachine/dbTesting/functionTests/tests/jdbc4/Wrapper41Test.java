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

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.CallableStatement;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;

/**
 * <p>
 * Machinery shared by the JDBC 4.1 tests for ResultSets and CallableStatements.
 * </p>
 */
public  class   Wrapper41Test   extends BaseJDBCTestCase
{
    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////

    private static  final   String  UNSUPPORTED_COERCION = "22005";
    private static  final   String  BAD_FORMAT = "22018";
    private static  final   String  BAD_DATETIME = "22007";

    private static  final   String  VARIABLE_STRING = "XXXXX";

    public  static  final   byte[]  BINARY_VALUE = new byte[] { (byte) 0xde };

    static final long TIME_VALUE = 83342000L;
    static final long TIMESTAMP_VALUE = -229527385766L;


    ///////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////

    private Class       _byteArrayClass;
    private boolean _rowOfNulls;

    ///////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////

    /**
     * Create test with given name.
     *
     * @param name name of the test.
     */
    public Wrapper41Test( String name ) { super( name ); }

    ///////////////////////////////////////////////////////////////////////
    //
    // BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////

    protected void examineJDBC4_1extensions( Wrapper41 wrapper, boolean rowOfNulls ) throws Exception
    {
        println( "Vetting a " + wrapper.getWrappedObject().getClass().getName() + ". rowOfNulls = " + rowOfNulls );

        _byteArrayClass = Class.forName( "[B" );
        _rowOfNulls = rowOfNulls;

        vetWrappedNull( wrapper );
        vetWrappedInteger( wrapper, 1, "BIGINTCOL" );
        vetWrappedBlob( wrapper );
        vetWrappedBoolean( wrapper );
        vetWrappedString( wrapper, 4, "CHARCOL" );
        vetWrappedBinary( wrapper, 5, "CHARFORBITDATACOL" );
        vetWrappedClob( wrapper );
        vetWrappedDate( wrapper );
        vetWrappedFloatingPoint( wrapper, 8, "DOUBLECOL" );
        vetWrappedFloatingPoint( wrapper, 9, "FLOATCOL" );
        vetWrappedInteger( wrapper, 10, "INTCOL" );
        vetWrappedString( wrapper, 11, "LONGVARCHARCOL" );
        vetWrappedBinary( wrapper, 12, "LONGVARCHARFORBITDATACOL" );
        vetWrappedInteger( wrapper, 13, "NUMERICCOL" );
        vetWrappedFloatingPoint( wrapper, 14, "REALCOL" );
        vetWrappedInteger( wrapper, 15, "SMALLINTCOL" );
        vetWrappedTime( wrapper );
        vetWrappedTimestamp( wrapper );
        vetWrappedString( wrapper, 18, "VARCHARCOL" );
        vetWrappedBinary( wrapper, 19, "VARCHARFORBITDATACOL" );
    }
    @SuppressWarnings("unchecked")
    private void    vetWrappedNull( Wrapper41 wrapper ) throws Exception
    {
            try {
                wrapper.getObject( 1, (Class) null );
                fail( "Did not expect to get a result for a null class type." );
            }
            catch (SQLException e)
            {
                assertSQLState( "Null type", UNSUPPORTED_COERCION, e );
            }

            // String overloads not implemented for CallableStatements
            if ( wrapper.getWrappedObject() instanceof CallableStatement ) { return; }
            
            try {
                wrapper.getObject( "BIGINTCOL", (Class) null );
                fail( "Did not expect to get a result for a null class type." );
            }
            catch (SQLException e)
            {
                assertSQLState( "Null type", UNSUPPORTED_COERCION, e );
            }
    }
    private void    vetWrappedInteger( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "1",
             new Class[] { String.class, BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class, Number.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "1.0",
             new Class[] { Float.class, Double.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "true",
             new Class[] { Boolean.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, _byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedBlob( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             2,
             "BLOBCOL",
             _rowOfNulls ? null : BINARY_VALUE,
             new Class[] { Blob.class, Object.class, _byteArrayClass, String.class,  }
             );
        
        vetNoWrapper
            (
             wrapper,
             2,
             "BLOBCOL",
             new Class[]
             {
                 BigDecimal.class, Boolean.class,
                 Byte.class, Short.class, Integer.class, Long.class, Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class
             }
             );

        //
        // We don't try to get a Clob value because we have already gotten a LOB value.
        // Trying to open another LOB stream raises an error. Using a random class type
        // also takes us down that code path, so we don't verify against getClass() either.
        //
    }
    private void    vetWrappedBoolean( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             _rowOfNulls ? null : "true",
             new Class[] { String.class, Boolean.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             _rowOfNulls ? null : "1",
             new Class[] { BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class }
             );
        vetWrapperOK
            (
             wrapper,
             3,
             "BOOLEANCOL",
             _rowOfNulls ? null : "1.0",
             new Class[] { Float.class, Double.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             3,
             "BOOLEANCOL",
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, _byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedString( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "a",
             new Class[] { String.class, Object.class }
             );

        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "true",
             new Class[] { Boolean.class }
             );

        vetCoercionError
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
             },
             BAD_FORMAT
             );

        vetCoercionError
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Date.class, Time.class, Timestamp.class
             },
             BAD_DATETIME
             );

        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Blob.class, Clob.class, _byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedBinary( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "de",
             new Class[] { String.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : BINARY_VALUE,
             new Class[] { _byteArrayClass, Object.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[]
             {
                 Boolean.class, BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, getClass()
             }
             );
    }
    private void    vetWrappedClob( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             6,
             "CLOBCOL",
             _rowOfNulls ? null : "abc",
             new Class[] { String.class, Clob.class, Object.class }
             );

        vetNoWrapper
            (
             wrapper,
             6,
             "CLOBCOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class, Time.class, Timestamp.class,
                 _byteArrayClass
             }
             );

        //
        // We don't test getting a BLOB because we are only allowed one attempt
        // to get a LOB from the column. Using a random class type
        // also takes us down that code path, so we don't verify against getClass() either.
        //
    }
    private void    vetWrappedDate( Wrapper41 wrapper ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             7,
             "DATECOL",
             _rowOfNulls ? null : "1994-02-23",
             new Class[] { String.class, Date.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             7,
             "DATECOL",
             _rowOfNulls ? null : "1994-02-23 00:00:00.0",
             new Class[] { Timestamp.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             7,
             "DATECOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Time.class,
                 Blob.class, Clob.class, _byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedFloatingPoint( Wrapper41 wrapper, int colID, String colName ) throws Exception
    {
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "1.0",
             new Class[] { String.class, Float.class, Double.class, BigDecimal.class, Number.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "1",
             new Class[] { Byte.class, Short.class, Integer.class, Long.class }
             );
        vetWrapperOK
            (
             wrapper,
             colID,
             colName,
             _rowOfNulls ? null : "true",
             new Class[] { Boolean.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             colID,
             colName,
             new Class[] { Date.class, Time.class, Timestamp.class, Blob.class, Clob.class, _byteArrayClass, getClass() }
             );
    }
    private void    vetWrappedTime( Wrapper41 wrapper ) throws Exception
    {
        Time expectedTime = new Time(TIME_VALUE);

        vetWrapperOK
            (
             wrapper,
             16,
             "TIMECOL",
             _rowOfNulls ? null : expectedTime.toString(),
             new Class[] { String.class, Time.class, Object.class }
             );
        vetWrapperOK
            (
             wrapper,
             16,
             "TIMECOL",
             _rowOfNulls ? null : timeToTimestamp(expectedTime).toString(),
             new Class[] { Timestamp.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             16,
             "TIMECOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Date.class,
                 Blob.class, Clob.class, _byteArrayClass, getClass()
             }
             );
    }
    private void    vetWrappedTimestamp( Wrapper41 wrapper ) throws Exception
    {
        String expectedTimestamp = new Timestamp(TIMESTAMP_VALUE).toString();
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             _rowOfNulls ? null : expectedTimestamp,
             new Class[] { String.class, Timestamp.class, Object.class }
             );

        String expectedTime = new Time(TIMESTAMP_VALUE).toString();
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             _rowOfNulls ? null : expectedTime,
             new Class[] { Time.class }
             );

        String expectedDate = new Date(TIMESTAMP_VALUE).toString();
        vetWrapperOK
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             _rowOfNulls ? null : expectedDate,
             new Class[] { Date.class }
             );
        
        vetNoWrapper
            (
             wrapper,
             17,
             "TIMESTAMPCOL",
             new Class[]
             {
                 Boolean.class,
                 BigDecimal.class, Byte.class, Short.class, Integer.class, Long.class,
                 Float.class, Double.class,
                 Blob.class, Clob.class, _byteArrayClass, getClass()
             }
             );
    }

    @SuppressWarnings("unchecked")
    private void    vetWrapperOK
        ( Wrapper41 wrapper, int colID, String colName, Object expectedValue, Class[] supportedCoercions )
        throws Exception
    {
        int coercionCount = supportedCoercions.length;
        for ( int i = 0; i < coercionCount; i++ )
        {
            Class   candidate = supportedCoercions[ i ];
            vetCandidate( candidate, expectedValue, wrapper.getObject( colID, candidate ) );
            
            // you can only retrieve a LOB once
            if ( (candidate == Blob.class) || (candidate == Clob.class) ) { return; }

            // String overloads not implemented for CallableStatement
            if ( !(wrapper.getWrappedObject() instanceof CallableStatement) )
            { vetCandidate( candidate, expectedValue, wrapper.getObject( colName, candidate ) ); }
        }
    }
    @SuppressWarnings("unchecked")
    private void    vetCandidate( Class candidate, Object expectedValue, Object actualValue )
        throws Exception
    {
        if ( actualValue != null ) { assertTrue( candidate.getName(), candidate.isAssignableFrom( actualValue.getClass( ) ) ); }

        if ( expectedValue == null )
        {
            assertNull( actualValue );
            return;
        }

        if ( VARIABLE_STRING.equals( expectedValue ) ) { return; }

        String  actualString;
        if ( actualValue instanceof Blob )
        {
            Blob    blob = (Blob) actualValue;
            vetBytes( (byte[]) expectedValue, blob.getBytes( 1L, (int) blob.length() ) );
            return;
        }
        else if ( actualValue instanceof byte[] )
        {
            vetBytes( (byte[]) expectedValue, (byte[]) actualValue );
            return;
        }
        else if ( actualValue instanceof Clob )
        {
            Clob    clob = (Clob) actualValue;
            actualString = clob.getSubString( 1L, (int) clob.length() );
        }
        else { actualString = actualValue.toString(); }
        
        assertEquals( candidate.getName(), (String) expectedValue, actualString );
    }
    private void    vetBytes( byte[] expected, byte[] actual ) throws Exception
    {
        int count = expected.length;

        assertEquals( count, actual.length );
        for ( int i = 0; i < count; i++ )
        {
            assertEquals( expected[ i ], actual[ i ] );
        }
    }
    private void    vetNoWrapper
        ( Wrapper41 wrapper, int colID, String colName, Class[] unsupportedCoercions )
        throws Exception
    {
        vetCoercionError( wrapper, colID, colName, unsupportedCoercions, UNSUPPORTED_COERCION );
    }
    @SuppressWarnings("unchecked")
    private void    vetCoercionError
        ( Wrapper41 wrapper, int colID, String colName, Class[] unsupportedCoercions, String expectedSQLState )
        throws Exception
    {
        // null can be coerced to anything
        if ( _rowOfNulls ) { return; }
        
        int coercionCount = unsupportedCoercions.length;
        for ( int i = 0; i < coercionCount; i++ )
        {
            Class   candidate = unsupportedCoercions[ i ];

            try {
                wrapper.getObject( colID, candidate );
                fail( "Did not expect to get a " + candidate.getName() );
            }
            catch (SQLException e)
            {
                assertSQLState( candidate.getName(), expectedSQLState, e );
            }

            // you can only retrieve a LOB once
            if ( (candidate == Blob.class) || (candidate == Clob.class) ) { return; }

            // String overloads not implemented for CallableStatements
            if ( wrapper.getWrappedObject() instanceof CallableStatement ) { return; }
            
            try {
                wrapper.getObject( colName, candidate );
                fail( "Did not expect to get a " + candidate.getName() );
            }
            catch (SQLException e)
            {
                assertSQLState( candidate.getName(), expectedSQLState, e );
            }
        }
    }
    
    ///////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////

    protected PreparedStatement    prepareStatement( Connection conn, String text )
        throws Exception
    {
        println( text );
        
        PreparedStatement   ps = conn.prepareStatement( text );

        return ps;
    }

    protected CallableStatement    prepareCall( Connection conn, String text )
        throws Exception
    {
        println( text );
        
        CallableStatement cs = conn.prepareCall( text );

        return cs;
    }

    /**
     * Convert a Time value to a Timestamp value the same way as when we call
     * getTimestamp() on a TIME column. That is, construct a Timestamp value
     * with the date component set to the current date and the time component
     * set to the specified time of day.
     *
     * @param time the Time value to convert
     * @return a Timestamp value representing the specified time on the
     * current date
     */
    private static Timestamp timeToTimestamp(Time time) {
        // Create a calendar object representing the time value
        Calendar timeCal = Calendar.getInstance();
        timeCal.setTime(time);

        // Create a calendar object for the timestamp, initialized with
        // the current time value
        Calendar tsCal = Calendar.getInstance();

        // Copy all fields, except the date fields, from the time calendar
        // to the timestamp calendar
        int[] timeFields = {
            Calendar.HOUR_OF_DAY,
            Calendar.MINUTE,
            Calendar.SECOND,
            Calendar.MILLISECOND
        };

        for (int field : timeFields) {
            tsCal.set(field, timeCal.get(field));
        }

        // Return a timestamp based on the current date and the specified time
        return new Timestamp(tsCal.getTimeInMillis());
    }

}
