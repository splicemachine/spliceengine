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

/**
 * A UDT which contains an array of ints.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;

public class IntArray implements Externalizable, Comparable
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

    private int[] _data;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public IntArray() {}

    public IntArray( int[] data )
    {
        _data = data;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static IntArray makeIntArray( int length )
    {
        return new IntArray( new int[ length ] );
    }

    public static IntArray setCell( IntArray array, int cellNumber, int cellValue )
    {
        array._data[ cellNumber ] = cellValue;

        return array;
    }

    public static int getCell( IntArray array, int cellNumber ) { return array._data[ cellNumber ]; }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Externalizable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        int length = _data.length;

        out.writeInt( length );

        for ( int i = 0; i < length; i++ ) { out.writeInt( _data[ i ] ); }
    }

    public void readExternal( ObjectInput in ) throws IOException
    {
        int length = in.readInt();

        _data = new int[ length ];

        for ( int i = 0; i < length; i++ ) { _data[ i ] = in.readInt(); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Comparable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public int compareTo( Object other )
    {
        if ( other == null ) { return -1; }
        if ( !( other instanceof IntArray) ) { return -1; }

        IntArray that = (IntArray) other;

        int minLength = (this._data.length <= that._data.length) ? this._data.length : that._data.length;

        int result;
        for ( int i = 0; i < minLength; i++ )
        {
            result = this._data[ i ] - that._data[ i ];

            if ( result != 0 ) { return result; }
        }

        result = this._data.length - that._data.length;

        return result;
    }

    public boolean equals( Object other ) { return ( compareTo( other ) == 0 ); }

    public int hashCode()
    {
        int firstValue;
        int secondValue;

        if ( _data.length== 0 )
        {
            firstValue = 1;
            secondValue = 1;
        }
        else
        {
            firstValue = _data[ 0 ];
            secondValue = _data[ _data.length -1 ];
        }

        return firstValue^secondValue;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER Object OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        StringBuffer buffer = new StringBuffer();
        int length = _data.length;

        buffer.append( "[ " );
        for ( int i = 0; i < length; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }
            buffer.append( _data[ i ] );
        }
        buffer.append( " ]" );

        return buffer.toString();
    }
}
