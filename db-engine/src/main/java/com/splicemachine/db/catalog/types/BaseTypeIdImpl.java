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

package com.splicemachine.db.catalog.types;

import com.splicemachine.db.catalog.TypeDescriptor;

import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.services.io.Formatable;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;

import com.splicemachine.db.iapi.types.TypeId;

import com.splicemachine.db.iapi.services.i18n.MessageService;
import com.splicemachine.db.iapi.reference.JDBC40Translation;
import com.splicemachine.db.iapi.reference.SQLState;

import com.splicemachine.db.iapi.util.IdUtil;

import java.sql.Types;

import java.io.ObjectOutput;
import java.io.ObjectInput;
import java.io.IOException;

/**
 * This class is the base class for all type ids that are written to the
 * system tables.
 */
public class BaseTypeIdImpl implements Formatable
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
    
    private int   formatId;

    // schema where the type lives. only for UDTs
    protected String schemaName;

    // unqualified type name
    String unqualifiedName;

    /**
     * JDBC type - derived from the format identifier.
     */
    transient int           JDBCTypeId;

    /**
     * niladic constructor. Needed for Formatable interface to work.
     *
     */

    public BaseTypeIdImpl() {}

    /**
     * 1 argument constructor. Needed for Formatable interface to work.
     *
     * @param formatId      Format id of specific type id.
     */
    public BaseTypeIdImpl(int formatId)
    {
        this.formatId = formatId;
        setTypeIdSpecificInstanceVariables();
    }

    /**
     * Constructor for an BaseTypeIdImpl
     *
     * @param SQLTypeName   The unqualified SQL name of the type
     */

    BaseTypeIdImpl(String SQLTypeName)
    {
        this.schemaName = null;
        this.unqualifiedName = SQLTypeName;
    }

    /**
     * Constructor for an BaseTypeIdImpl which describes a UDT
     *
     * @param schemaName The schema that the UDT lives in
     * @param unqualifiedName The unqualified name of the UDT in that schema
     */

    BaseTypeIdImpl(String schemaName, String unqualifiedName )
    {
        this.schemaName = schemaName;
        this.unqualifiedName = unqualifiedName;
    }

    /**
     * Returns the SQL name of the datatype. If it is a Derby user-defined type,
     * it returns the full Java path name for the datatype, meaning the
     * dot-separated path including the package names. If it is a UDT, returns
     * "schemaName"."unqualifiedName".
     *
     * @return      A String containing the SQL name of this type.
     */
    public String   getSQLTypeName()
    {
        if ( schemaName == null ) { return unqualifiedName; }
        else { return IdUtil.mkQualifiedName( schemaName, unqualifiedName ); }
    }

    /** Get the schema name of this type. Non-null only for UDTs */
    public String getSchemaName() { return schemaName; }

    /** Get the unqualified name of this type. Except for UDTs, this is the same
     * value as getSQLTypeName()
     */
    public String getUnqualifiedName() { return unqualifiedName; }

    /** Return true if this is this type id describes an ANSI UDT */
    public boolean isAnsiUDT() { return (schemaName != null); }
    
    /**
     * Get the jdbc type id for this type.  JDBC type can be
     * found in java.sql.Types. 
     *
     * @return      a jdbc type, e.g. java.sql.Types.DECIMAL 
     *
     * @see Types
     */
    public int getJDBCTypeId()
    {
        return JDBCTypeId;
    }

    /**
     * Converts this TypeId, given a data type descriptor 
     * (including length/precision), to a string. E.g.
     *
     *                      VARCHAR(30)
     *
     *
     *      For most data types, we just return the SQL type name.
     *
     *      @param  td      Data type descriptor that holds the 
     *                      length/precision etc. as necessary
     *
     *       @return        String version of datatype, suitable for running 
     *                      through the Parser.
     */
    public String   toParsableString(TypeDescriptor td)
    {
        String retval = getSQLTypeName();

        switch (getTypeFormatId())
        {
          case StoredFormatIds.BIT_TYPE_ID_IMPL:
          case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
			  int rparen = retval.indexOf(')');
			  String lead = retval.substring(0, rparen);
			  retval = lead + td.getMaximumWidth() + retval.substring(rparen);
			  break;

          case StoredFormatIds.CHAR_TYPE_ID_IMPL:
          case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
                retval += "(" + td.getMaximumWidth() + ")";
                break;

          case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
                retval += "(" + td.getPrecision() + "," + td.getScale() + ")";
                break;
        }

        return retval;
    }

    /** Does this type id represent a user type? */
    public boolean userType()
    {
        return false;
    }

    /**
     * Format this BaseTypeIdImpl as a String
     *
     * @return      This BaseTypeIdImpl formatted as a String
     */

    public String   toString()
    {
        return MessageService.getTextMessage(SQLState.TI_SQL_TYPE_NAME) +
                ": " + getSQLTypeName();
    }

    /**
     * we want equals to say if these are the same type id or not.
     */
    public boolean equals(Object that)
    {
        if (that instanceof BaseTypeIdImpl)
        {
            return this.getSQLTypeName().equals(((BaseTypeIdImpl)that).getSQLTypeName());
        }
        else
        {
            return false;
        }
    }

    /**
      Hashcode which works with equals.
      */
    public int hashCode()
    {
        return this.getSQLTypeName().hashCode();
    }

    /**
     * Get the formatID which corresponds to this class.
     *
     * @return      the formatID of this class
     */
    public int getTypeFormatId()
    {
        if ( formatId != 0 ) { return formatId; }
        else
        {
            //
            // If you serialize this class outside the formatable machinery, you
            // will lose the format id. This can happen if you pass one of these
            // objects across the network. Here we recover the format id.
            //
            if ( TypeId.BOOLEAN_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.BOOLEAN_TYPE_ID_IMPL; }
            else if ( TypeId.LONGINT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.LONGINT_TYPE_ID_IMPL; }
            else if ( TypeId.INTEGER_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.INT_TYPE_ID_IMPL; }
            else if ( TypeId.SMALLINT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.SMALLINT_TYPE_ID_IMPL; }
            else if ( TypeId.TINYINT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.TINYINT_TYPE_ID_IMPL; }
            else if ( TypeId.LONGINT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.LONGINT_TYPE_ID_IMPL; }
            else if ( TypeId.DECIMAL_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.DECIMAL_TYPE_ID_IMPL; }
            else if ( TypeId.NUMERIC_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.DECIMAL_TYPE_ID_IMPL; }
            else if ( TypeId.DOUBLE_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.DOUBLE_TYPE_ID_IMPL; }
            else if ( TypeId.REAL_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.REAL_TYPE_ID_IMPL; }
            else if ( TypeId.REF_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.REF_TYPE_ID_IMPL; }
            else if ( TypeId.CHAR_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.CHAR_TYPE_ID_IMPL; }
            else if ( TypeId.VARCHAR_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.VARCHAR_TYPE_ID_IMPL; }
            else if ( TypeId.LONGVARCHAR_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL; }
            else if ( TypeId.CLOB_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.CLOB_TYPE_ID_IMPL; }
            //DERBY-5407 Network Server on wire sends CHAR () FOR BIT DATA 
            // not CHAR FOR BIT DATA. Keeping the check for CHAR FOR BIT
            // DATA just in case if there is any dependency on that check
            else if ( TypeId.BIT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.BIT_TYPE_ID_IMPL; }
            else if ( "CHAR FOR BIT DATA".equals( unqualifiedName ) ) { return StoredFormatIds.BIT_TYPE_ID_IMPL; }
            //DERBY-5407 Network Server on wire sends VARCHAR () FOR BIT DATA 
            // not VARCHAR FOR BIT DATA. Keeping the check for VARCHAR FOR BIT
            // DATA just in case if there is any dependency on that check
            else if ( TypeId.VARBIT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.VARBIT_TYPE_ID_IMPL; }
            else if ( "VARCHAR FOR BIT DATA".equals( unqualifiedName ) ) { return StoredFormatIds.VARBIT_TYPE_ID_IMPL; }
            else if ( TypeId.LONGVARBIT_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL; }
            else if ( TypeId.BLOB_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.BLOB_TYPE_ID_IMPL; }
            else if ( TypeId.DATE_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.DATE_TYPE_ID_IMPL; }
            else if ( TypeId.TIME_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.TIME_TYPE_ID_IMPL; }
            else if ( TypeId.TIMESTAMP_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL; }
            else if ( TypeId.XML_NAME.equals( unqualifiedName ) ) { return StoredFormatIds.XML_TYPE_ID_IMPL; }
            else { return 0; }
        }
    }

    /**
     * Read this object from a stream of stored objects.
     *
     * @param in read this.
     *
     * @exception IOException                       thrown on error
     * @exception ClassNotFoundException            thrown on error
     */
    public void readExternal( ObjectInput in )
             throws IOException, ClassNotFoundException
    {
    	JDBCTypeId = in.readInt();
    	formatId = in.readInt();
        unqualifiedName = in.readUTF();

        //
        // If the name begins with a quote, then it is just the first part
        // of the type name, viz., the schema that the type lives in.
        // Strip the quotes from around the name and then read the
        // following  unqualified name.
        //
        if ( unqualifiedName.charAt( 0 ) == '"' )
        {
            schemaName = stripQuotes( unqualifiedName );
            unqualifiedName = in.readUTF();
        }
    }

    /**
     * Write this object to a stream of stored objects.
     *
     * @param out write bytes here.
     *
     * @exception IOException               thrown on error
     */
    public void writeExternal( ObjectOutput out )
             throws IOException
    {
    	out.writeInt(JDBCTypeId);
    	out.writeInt(formatId);
        if ( schemaName == null ) { out.writeUTF( unqualifiedName ); }
        else
        {
            //
            // Wrap the schema name in quotes. quotes are illegal characters in
            // basic SQL type names and in Java class names, so this will flag
            // readExternal() that this type has a 2-part name
            // (schemaName.unqualifiedName).
            //
            out.writeUTF( doubleQuote( schemaName ) );
            out.writeUTF( unqualifiedName );
        }
    }

    private void setTypeIdSpecificInstanceVariables()
    {
        switch (getTypeFormatId())
        {
          case StoredFormatIds.BOOLEAN_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.BOOLEAN_NAME;
              JDBCTypeId = Types.BOOLEAN;
              break;

          case StoredFormatIds.INT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.INTEGER_NAME;
              JDBCTypeId = Types.INTEGER;
              break;

          case StoredFormatIds.SMALLINT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.SMALLINT_NAME;
              JDBCTypeId = Types.SMALLINT;
              break;

          case StoredFormatIds.TINYINT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.TINYINT_NAME;
              JDBCTypeId = Types.TINYINT;
              break;

          case StoredFormatIds.LONGINT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.LONGINT_NAME;
              JDBCTypeId = Types.BIGINT;
              break;

          case StoredFormatIds.DECIMAL_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.DECIMAL_NAME;
              JDBCTypeId = Types.DECIMAL;
              break;

          case StoredFormatIds.DOUBLE_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.DOUBLE_NAME;
              JDBCTypeId = Types.DOUBLE;
              break;

          case StoredFormatIds.REAL_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.REAL_NAME;
              JDBCTypeId = Types.REAL;
              break;
                
          case StoredFormatIds.REF_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.REF_NAME;
              JDBCTypeId = Types.REF;
              break;

          case StoredFormatIds.CHAR_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.CHAR_NAME;
              JDBCTypeId = Types.CHAR;
              break;

          case StoredFormatIds.VARCHAR_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.VARCHAR_NAME;
              JDBCTypeId = Types.VARCHAR;
              break;

          case StoredFormatIds.LONGVARCHAR_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.LONGVARCHAR_NAME;
              JDBCTypeId = Types.LONGVARCHAR;
              break;

          case StoredFormatIds.CLOB_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.CLOB_NAME;
              JDBCTypeId = Types.CLOB;
              break;

          case StoredFormatIds.BIT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.BIT_NAME;
              JDBCTypeId = Types.BINARY;
              break;

          case StoredFormatIds.VARBIT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.VARBIT_NAME;
              JDBCTypeId = Types.VARBINARY;
              break;

          case StoredFormatIds.LONGVARBIT_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.LONGVARBIT_NAME;
              JDBCTypeId = Types.LONGVARBINARY;
              break;

          case StoredFormatIds.BLOB_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.BLOB_NAME;
              JDBCTypeId = Types.BLOB;
              break;

          case StoredFormatIds.DATE_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.DATE_NAME;
              JDBCTypeId = Types.DATE;
              break;

          case StoredFormatIds.TIME_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.TIME_NAME;
              JDBCTypeId = Types.TIME;
              break;

          case StoredFormatIds.TIMESTAMP_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.TIMESTAMP_NAME;
              JDBCTypeId = Types.TIMESTAMP;
              break;

          case StoredFormatIds.XML_TYPE_ID_IMPL:
              schemaName = null;
              unqualifiedName = TypeId.XML_NAME;
              JDBCTypeId = JDBC40Translation.SQLXML;
              break;

          default:
                if (SanityManager.DEBUG)
                {
                    SanityManager.THROWASSERT("Unexpected formatId " + getTypeFormatId());
                }
                break;
        }
    }

    // wrap a string in quotes
    private String doubleQuote( String raw ) { return '"' + raw + '"'; }

    // strip the bracketing quotes from a string
    private String stripQuotes( String quoted ) { return quoted.substring( 1, quoted.length() - 1 ); }
    
}
