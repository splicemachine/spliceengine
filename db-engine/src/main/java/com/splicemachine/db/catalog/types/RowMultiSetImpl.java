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

package com.splicemachine.db.catalog.types;
import com.splicemachine.db.catalog.TypeDescriptor;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;
import java.sql.Types;
import java.util.Arrays;

/**
 * <p>
 * Row data type as described in the 2003 SQL spec
 * in part 2, section 4.8.
 * </p>
 */
public class RowMultiSetImpl extends BaseTypeIdImpl
{
    /********************************************************
    **
    **      This class implements Formatable. That means that it
    **      can write itself to and from a formatted stream. If
    **      you add more fields to this class, make sure that you
    **      also write/read them with the writeExternal()/readExternal()
    **      methods.
    **
    **      If, inbetween releases, you add more fields to this class,
    **      then you should bump the version number emitted by the 
    **      getTypeFormatId() method.
    **
    ********************************************************/

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

    private String[]                            _columnNames;
    private TypeDescriptor[]    _types;

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * 0-arg constructor for Formatable machinery.
     * </p>
     */
    public RowMultiSetImpl()
    {}
    
    /**
     * <p>
     * Construct from column names and their types.
     * </p>
     */
    public RowMultiSetImpl( String[] columnNames, TypeDescriptor[] types )
    {
        _columnNames = columnNames;
        _types = types;

        if (
            (columnNames == null ) ||
            (types == null) ||
            (columnNames.length != types.length )
            )
        {
            throw new IllegalArgumentException( "Bad args: columnNames = " + Arrays.toString(columnNames) + ". types = " + Arrays.toString(types));
        }
    }

        
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // ACCESSORS
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /** Get the names of the columns in this row set */
    public  String[]    getColumnNames()    { return _columnNames; }
    
    /** Get the types of the columns in this row set */
    public  TypeDescriptor[]    getTypes() { return _types; }
    
    ///////////////////////////////////////////////////////////////////////////////////
    //
    // OVERRIDE BEHAVIOR IN BaseTypeIdImpl
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the SQL name of this multi set. This is the name suitable for
     * replaying the DDL to create a Table Function.
     * </p>
     */
    public  String  getSQLTypeName()
    {
        StringBuilder buffer = new StringBuilder();
        int                     count = _columnNames.length;

        buffer.append( "TABLE ( " );

        for ( int i = 0; i < count; i++ )
        {
            if ( i > 0 ) { buffer.append( ", " ); }

            buffer.append( '\"' );
            buffer.append( _columnNames[ i ] );
            buffer.append( '\"' );
            buffer.append( ' ' );
            buffer.append( _types[ i ].getSQLstring() );
        }

        buffer.append( " )" );

        return buffer.toString();
    }
    
    /**
     * <p>
     * Get the corresponding JDBC type.
     * </p>
     */
    public  int getJDBCTypeId()
    {
        return Types.OTHER;
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // Formatable BEHAVIOR
    //
    ///////////////////////////////////////////////////////////////////////////////////

    /**
     * <p>
     * Get the id which indicates which class deserializes us.
     * </p>
     */
    public  int getTypeFormatId()
    {
        return StoredFormatIds.ROW_MULTISET_TYPE_ID_IMPL;
    }
    
    /**
     * <p>
     * Read ourself from a formatable stream.
     * </p>
     */
    public  void readExternal( ObjectInput in )
        throws IOException, ClassNotFoundException
    {
        int     count = in.readInt();

        _columnNames = new String[ count ];
        _types = new TypeDescriptor[ count ];

        for ( int i = 0; i < count; i++ ) { _columnNames[ i ] = in.readUTF(); }
        for ( int i = 0; i < count; i++ ) { _types[ i ] = (TypeDescriptor) in.readObject(); }
    }

    /**
     * <p>
     * Write ourself to a formatable stream.
     * </p>
     */
    public  void writeExternal( ObjectOutput out )
        throws IOException
    {
        int     count = _columnNames.length;

        out.writeInt( count );

        for (String _columnName : _columnNames) {
            out.writeUTF(_columnName);
        }
        for ( int i = 0; i < count; i++ ) { out.writeObject( _types[ i ] ); }
    }

    ///////////////////////////////////////////////////////////////////////////////////
    //
    // MINIONS
    //
    ///////////////////////////////////////////////////////////////////////////////////



}
