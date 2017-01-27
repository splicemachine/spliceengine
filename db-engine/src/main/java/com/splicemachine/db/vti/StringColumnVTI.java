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

package com.splicemachine.db.vti;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;

import com.splicemachine.db.iapi.types.HarmonySerialBlob;
import com.splicemachine.db.iapi.types.HarmonySerialClob;

/**
 * <p>
 * This is an abstract table function which assumes that all columns are strings and which
 * coerces the strings to reasonable values for various getXXX()
 * methods. Subclasses must implement the following ResultSet methods:
 * </p>
 *
 * <ul>
 * <li>next( )</li>
 * <li>close()</li>
 * </ul>
 *
 * <p>
 * and the following protected method introduced by this class:
 * </p>
 *
 * <ul>
 * <li>getRawColumn( int columnNumber )</li>
 * </ul>
 */
public  abstract    class   StringColumnVTI extends VTITemplate
{
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // STATE
    //
    ///////////////////////////////////////////////////////////////////////////////////

    private String[]      _columnNames;
    private boolean _lastColumnWasNull;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // StringColumnVTI BEHAVIOR TO BE IMPLEMENTED BY SUBCLASSES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the string value of the column in the current row identified by the 1-based columnNumber.
     * </p>
     */
    protected  abstract    String  getRawColumn( int columnNumber ) throws SQLException;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Build a StringColumnVTI with the given column names
     * </p>
     */
    public  StringColumnVTI( String[] columnNames )
    {
        _columnNames = columnNames;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the number of columns.
     * </p>
     */
    public int getColumnCount() { return _columnNames.length; }

    /**
     * <p>
     * Get name of a column (1-based indexing).
     * </p>
     */
    public String getColumnName( int columnNumber ) { return _columnNames[ columnNumber - 1 ]; }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ResultSet BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public boolean wasNull() throws SQLException { return _lastColumnWasNull; }
    
    public int findColumn( String columnName ) throws SQLException
    {
        int     count = _columnNames.length;

        for ( int i = 0; i < count; i++ ) { if ( _columnNames[ i ].equals( columnName ) ) { return i+1; } }

        throw new SQLException( "Unknown column name." );
    }
    
    public String getString(int columnIndex) throws SQLException
    {
        String  columnValue = getRawColumn( columnIndex );

        checkNull( columnValue );

        return columnValue;
    }
    
    public boolean getBoolean(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return false; }
        else { return Boolean.valueOf( columnValue ).booleanValue(); }
    }

    public byte getByte(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (byte) 0; }
        else
        {
            try {
                return Byte.valueOf( columnValue ).byteValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public short getShort(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (short) 0; }
        else
        {
            try {
                return Short.valueOf( columnValue ).shortValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public int getInt(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return 0; }
        else
        {
            try {
                return Integer.parseInt( columnValue );
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public long getLong(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (long) 0; }
        else
        {
            try {
                return Long.valueOf( columnValue ).longValue();
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

   public float getFloat(int columnIndex) throws SQLException
   {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (float) 0; }
        else
        {
            try {
                return Float.parseFloat( columnValue );
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public double getDouble(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return (double) 0; }
        else
        {
            try {
                return Double.parseDouble( columnValue );
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public BigDecimal getBigDecimal(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            try {
                return new BigDecimal( columnValue );
            } catch (NumberFormatException e) { throw wrap( e ); }
        }
    }

    public byte[] getBytes(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            try {
                return columnValue.getBytes( "UTF-8" );
            } catch (Throwable t) { throw new SQLException( t.getMessage() ); }
        }
    }

    public java.sql.Date getDate(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Date( parseDateTime( columnValue ) );
        }
    }

    public java.sql.Time getTime(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Time( parseDateTime( columnValue ) );
        }
    }

    public java.sql.Timestamp getTimestamp(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else
        {
            return new Timestamp( parseDateTime( columnValue ) );
        }
    }

    public InputStream getAsciiStream(int columnIndex) throws SQLException
    {
        String  columnValue = getString( columnIndex );

        return getEncodedStream( columnValue, "US-ASCII" );
    }

    public java.io.InputStream getBinaryStream(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else { return new ByteArrayInputStream( getBytes( columnIndex ) ); }
    }

    public Blob getBlob(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        else { return new HarmonySerialBlob( getBytes( columnIndex ) ); }
    }
    
    public Clob getClob(int columnIndex)
        throws SQLException
    {
        String  columnValue = getString( columnIndex );

        if ( columnValue == null ) { return null; }
        { return new HarmonySerialClob( getString( columnIndex ) ); }
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Set the wasNull flag based on whether this column value turned out to be null.
     * </p>
     */
    private void checkNull( String columnValue )
    {
        _lastColumnWasNull = ( columnValue == null );
    }
    
    /**
     * <p>
     * Wrap an exception in a SQLException.
     * </p>
     */
    private SQLException wrap( Throwable t )
    {
        return new SQLException( t.getMessage() );
    }
    
    /**
     * <p>
     * Translate a date/time expression into the corresponding long number of
     * milliseconds.
     * </p>
     */
    private long parseDateTime( String columnValue  )
        throws SQLException
    {
        try {
            DateFormat      df = DateFormat.getDateTimeInstance();
                
            java.util.Date  rawDate = df.parse( columnValue );

            return rawDate.getTime();
        } catch (ParseException e) { throw wrap( e ); }
    }
    
    /**
     * <p>
     * Turn a string into an appropriately encoded ByteArrayInputStream.
     * </p>
     */
    private InputStream getEncodedStream( String columnValue, String encoding  )
        throws SQLException
    {
        if ( columnValue == null ) { return null; }
        else
        {
            try {
                byte[]      rawBytes = columnValue.getBytes( encoding );
            
                return new ByteArrayInputStream( rawBytes );
            } catch (UnsupportedEncodingException e) { throw wrap( e ); }
        }
    }

}
