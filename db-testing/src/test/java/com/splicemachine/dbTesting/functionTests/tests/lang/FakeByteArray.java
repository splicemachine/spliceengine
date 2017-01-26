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
 * A UDT which can serialize a lot of bytes.
 */
package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectInput;

public class FakeByteArray implements Externalizable
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

    private int _length;
    private byte _fill;
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public FakeByteArray() {}

    public FakeByteArray( int length, int fill )
    {
        _length = length;
        _fill = (byte) fill;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // FUNCTIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public static FakeByteArray makeFakeByteArray( int length, int biggestByte )
    {
        return new FakeByteArray( length, biggestByte );
    }

    public static String toString( FakeByteArray data ) { return data.toString(); }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Externalizable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public void writeExternal( ObjectOutput out ) throws IOException
    {
        out.writeInt( _length );

        for ( int i = 0; i < _length; i++ ) { out.write( _fill ); }
    }

    public void readExternal( ObjectInput in ) throws IOException
    {
        _length = in.readInt();

        for ( int i = 0; i < _length; i++ ) { _fill = (byte) in.read(); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OTHER Object OVERRIDES
    //
    ///////////////////////////////////////////////////////////////////////////////////

    public String toString()
    {
        return "[ " + _length + ", " + _fill + " ]";
    }
}
