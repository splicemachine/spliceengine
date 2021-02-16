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

import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.catalog.TypeDescriptor;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.util.IdUtil;

/**
 * Describe a routine (procedure or function) alias.
 *
 * @see com.splicemachine.db.catalog.AliasInfo
 */
public class RoutineAliasInfo extends MethodAliasInfo
{

	private static final String[] SQL_CONTROL = {"MODIFIES SQL DATA", "READS SQL DATA", "CONTAINS SQL", "NO SQL"};
	public static final short MODIFIES_SQL_DATA = 0;
	public static final short READS_SQL_DATA	= 1;
	public static final short CONTAINS_SQL		= 2;
	public static final short NO_SQL			= 3;



	/** PARAMETER STYLE JAVA */
	public static final short PS_JAVA = 0;

	/** PARAMETER STYLE SPLICE_JDBC_RESULT_SET */
	public static final short PS_SPLICE_JDBC_RESULT_SET = PS_JAVA + 1;

    /** Masks for the sqlOptions field */
    private static final short SQL_ALLOWED_MASK = (short) 0xF;
    private static final short DETERMINISTIC_MASK = (short) 0x10;

    /** Mask for the SECURITY INVOKER/DEFINER field */
    private static final short SECURITY_DEFINER_MASK = (short) 0x20;

	/** Number of fields added to RoutineAliasInfo
	 * used for serialization and deserialization */
	private int expansionNum = 1;

    private String language;

    private int parameterCount;

    /**
     * Types of the parameters. If there are no parameters
     * then this may be null (or a zero length array).
     */
	private TypeDescriptor[]	parameterTypes;
        /**
         * Name of each parameter. As of DERBY 10.3, parameter names
         * are optional. If the parameter is unnamed, parameterNames[i]
         * is a string of length 0
         */
	private String[]			parameterNames;
	/**
		IN, OUT, INOUT
	*/
	private int[]				parameterModes;

	private int dynamicResultSets;

	/**
		Return type for functions. Null for procedures.
	*/
	private TypeDescriptor	returnType;

	/**
		Parameter style - always PS_JAVA at the moment.
	*/
	private short parameterStyle;

	/**
		This field contains several pieces of information:

        bits 0-3    sqlAllowed = MODIFIES_SQL_DATA, READS_SQL_DATA,CONTAINS_SQL, or NO_SQL

        bit 4         on if function is DETERMINISTIC, off otherwise
        bit 5         on if running with definer's right, off otherwise
    */
	private short	sqlOptions;

	/**
		SQL Specific name (future)
	*/
	private String	specificName;

	/**
		True if the routine is called on null input.
		(always true for procedures).
	*/
	private boolean	calledOnNullInput;

	// What type of alias is this: PROCEDURE or FUNCTION?
	private transient char aliasType;

	// The bytes for compiled Python script
	private byte[] compiledPyCode;

	public RoutineAliasInfo() {
	}

	/**
		Create a RoutineAliasInfo for an internal PROCEDURE.
	*/
	public RoutineAliasInfo(String methodName, String language,int parameterCount, String[] parameterNames,
                            TypeDescriptor[]	parameterTypes, int[] parameterModes, int dynamicResultSets, short parameterStyle, short sqlAllowed,
                            boolean isDeterministic ) {

        this(methodName,
             language,
             parameterCount,
             parameterNames,
             parameterTypes,
             parameterModes,
             dynamicResultSets,
             parameterStyle,
             sqlAllowed,
             isDeterministic,
             false /* definersRights*/,
             true,
             (TypeDescriptor) null,
				null);
	}

	/**
		Create a RoutineAliasInfo for a PROCEDURE or FUNCTION
	*/
    public RoutineAliasInfo(String methodName,
                            String language,
                            int parameterCount,
                            String[] parameterNames,
                            TypeDescriptor[] parameterTypes,
                            int[] parameterModes,
                            int dynamicResultSets,
                            short parameterStyle,
                            short sqlAllowed,
                            boolean isDeterministic,
                            boolean definersRights,
                            boolean calledOnNullInput,
                            TypeDescriptor returnType,
							byte[] compiledPyCode)
	{

		super(methodName);
		this.language = language;
		this.parameterCount = parameterCount;
		this.parameterNames = parameterNames;
		this.parameterTypes = parameterTypes;
		this.parameterModes = parameterModes;
		this.dynamicResultSets = dynamicResultSets;
		this.parameterStyle = parameterStyle;
		this.sqlOptions = (short) (sqlAllowed & SQL_ALLOWED_MASK);
        if ( isDeterministic ) { this.sqlOptions = (short) (sqlOptions | DETERMINISTIC_MASK); }

        if (definersRights) {
            this.sqlOptions = (short) (sqlOptions | SECURITY_DEFINER_MASK);
        }

		this.calledOnNullInput = calledOnNullInput;
		this.returnType = returnType;
		this.compiledPyCode = compiledPyCode;

		if (SanityManager.DEBUG) {

			if (parameterCount != 0 && parameterNames.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterNames array " + parameterNames.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterNames != null && parameterNames.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterNames array " + " not zero " + " != " + parameterCount);
			}

			if (parameterCount != 0 && parameterTypes.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterTypes array " + parameterTypes.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterTypes != null && parameterTypes.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterTypes array " + " not zero " + " != " + parameterCount);
			}

			if (parameterCount != 0 && parameterModes.length != parameterCount) {
				SanityManager.THROWASSERT("Invalid parameterModes array " + parameterModes.length + " != " + parameterCount);
			}
			else if (parameterCount == 0 && parameterModes != null && parameterModes.length != 0) {
				SanityManager.THROWASSERT("Invalid parameterModes array " + " not zero " + " != " + parameterCount);
			}

			if (returnType != null) {
				if (!((sqlAllowed >= RoutineAliasInfo.READS_SQL_DATA) && (sqlAllowed <= RoutineAliasInfo.NO_SQL))) {
					SanityManager.THROWASSERT("Invalid sqlAllowed for FUNCTION " + methodName + " " + sqlAllowed);
				}
			} else {
				if (!((sqlAllowed >= RoutineAliasInfo.MODIFIES_SQL_DATA) && (sqlAllowed <= RoutineAliasInfo.NO_SQL))) {
					SanityManager.THROWASSERT("Invalid sqlAllowed for PROCEDURE " + methodName + " " + sqlAllowed);
				}
				
			}
		}
	}

	public String getLanguage() { return language; }

	public int getParameterCount() {
		return parameterCount;
	}

    /**
     * Types of the parameters. If there are no parameters
     * then this may return null (or a zero length array).
     */
	public TypeDescriptor[] getParameterTypes() {
		return parameterTypes;
	}

	public int[] getParameterModes() {
		return parameterModes;
	}
        /**
         * Returns an array containing the names of the parameters.
         * As of DERBY 10.3, parameter names are optional (see DERBY-183
         * for more information). If the i-th parameter was unnamed,
         * parameterNames[i] will contain a string of length 0.
         */
	public String[] getParameterNames() {
		return parameterNames;
	}

	public int getMaxDynamicResultSets() {
		return dynamicResultSets;
	}

	public short getParameterStyle() {
		return parameterStyle;
	}

	public short getSQLAllowed() {
		return (short) (sqlOptions & SQL_ALLOWED_MASK);
	}

	public byte[] getCompiledPyCode() { return compiledPyCode;}

    public boolean isDeterministic()
    {
        return ( (sqlOptions & DETERMINISTIC_MASK) != 0 );
    }

    public boolean hasDefinersRights()
    {
        return ( (sqlOptions & SECURITY_DEFINER_MASK) != 0 );
    }

	public boolean calledOnNullInput() {
		return calledOnNullInput;
	}

	public TypeDescriptor getReturnType() {
		return returnType;
	}

	public boolean isTableFunction() {
        return returnType != null && returnType.isRowMultiSet();
    }


	// Formatable methods

	/**
	 * Read this object from a stream of stored objects.
	 *
	 * @param in read this.
	 *
	 * @exception IOException					thrown on error
	 * @exception ClassNotFoundException		thrown on error
	 */
	public void readExternal( ObjectInput in )
		 throws IOException, ClassNotFoundException
	{
		super.readExternal(in);
		specificName = (String) in.readObject();
		dynamicResultSets = in.readInt();
		parameterCount = in.readInt();
		parameterStyle = in.readShort();
		sqlOptions = in.readShort();
		returnType = getStoredType(in.readObject());
		calledOnNullInput = in.readBoolean();
		// expansionNum is used for adding more fields in the future.
		// It is an indicator for whether extra fields exist and need
		// to be written
		expansionNum = in.readInt();

		if (parameterCount != 0) {
			parameterNames = new String[parameterCount];
			parameterTypes = new TypeDescriptor[parameterCount];

			ArrayUtil.readArrayItems(in, parameterNames);
            for (int p = 0; p < parameterTypes.length; p++)
            {
                parameterTypes[p] = getStoredType(in.readObject());
            }
			parameterModes = ArrayUtil.readIntArray(in);

		} else {
			parameterNames = null;
			parameterTypes = null;
			parameterModes = null;
		}
		if(expansionNum == 1){
			language = (String) in.readObject();
			compiledPyCode = (byte[]) in.readObject();
		}
	}
    
    /**
     * Old releases (10.3 and before) wrote out the runtime
     * DataTypeDescriptor for routine parameter and return types.
     * 10.4 onwards (DERBY-2775) always writes out the catalog
     * type TypeDescriptor. Here we see what object was read from
     * disk and if it was the old type, now mapped to OldRoutineType,
     * we extract the catalog type and use that.
     * 
     * @param onDiskType The object read that represents the type.
     * @return A type descriptor.
     */
    public static TypeDescriptor getStoredType(Object onDiskType)
    {
        if (onDiskType instanceof OldRoutineType)
            return ((OldRoutineType) onDiskType).getCatalogType();
        return (TypeDescriptor) onDiskType;
    }

	/**
	 * Write this object to a stream of stored objects.
	 *
	 * @param out write bytes here.
	 *
	 * @exception IOException		thrown on error
	 */
	public void writeExternal( ObjectOutput out )
		 throws IOException
	{
		super.writeExternal(out);
		out.writeObject(specificName);
		out.writeInt(dynamicResultSets);
		out.writeInt(parameterCount);
		out.writeShort(parameterStyle);
		out.writeShort(sqlOptions);
		out.writeObject(returnType);
		out.writeBoolean(calledOnNullInput);
		// expansionNum is used for adding more fields in the future.
		// It is an indicator for whether extra fields exist
		out.writeInt(expansionNum);
		if (parameterCount != 0) {
			ArrayUtil.writeArrayItems(out, parameterNames);
			ArrayUtil.writeArrayItems(out, parameterTypes);
			ArrayUtil.writeIntArray(out, parameterModes);
		}
		if(expansionNum==1){
			out.writeObject(language);
			out.writeObject(compiledPyCode);
		}
	}
 
	/**
	 * Get the formatID which corresponds to this class.
	 *
	 *	@return	the formatID of this class
	 */
	public	int	getTypeFormatId()	{ return StoredFormatIds.ROUTINE_INFO_V01_ID; }

	/**
	 * Get this alias info as a string.  NOTE: The "ALIASINFO" column
	 * in the SYSALIASES table will return the result of this method
	 * on a ResultSet.getString() call.  That said, since the dblook
	 * utility uses ResultSet.getString() to retrieve ALIASINFO and
	 * to generate the DDL, THIS METHOD MUST RETURN A STRING THAT
	 * IS SYNTACTICALLY VALID, or else the DDL generated by dblook
	 * will be incorrect.
	 */
	public String toString() {

		StringBuilder sb = new StringBuilder(100);
		sb.append(getMethodName());
		sb.append('(');
		for (int i = 0; i < parameterCount; i++) {
			if (i != 0)
				sb.append(',');

			if (returnType == null) {
			// This is a PROCEDURE.  We only want to print the
			// parameter mode (ex. "IN", "OUT", "INOUT") for procedures--
			// we don't do it for functions since use of the "IN" keyword
			// is not part of the FUNCTION syntax.
				sb.append(RoutineAliasInfo.parameterMode(parameterModes[i]));
				sb.append(' ');
			}
			sb.append(IdUtil.normalToDelimited(parameterNames[i]));
			sb.append(' ');
			sb.append(parameterTypes[i].getSQLstring());
		}
		sb.append(')');

		if (returnType != null) {
		// this a FUNCTION, so syntax requires us to append the return type.
			sb.append(" RETURNS ").append(returnType.getSQLstring());
		}

		sb.append(" LANGUAGE ");
		sb.append(this.language);	// Language can now be Python
		sb.append(" PARAMETER STYLE " );

		switch( parameterStyle )
		{
		    case PS_JAVA:    sb.append( "JAVA " ); break;
		    case PS_SPLICE_JDBC_RESULT_SET:    sb.append( "SPLICE_JDBC_RESULT_SET " ); break;
		}
        
        if ( isDeterministic() )
        { sb.append( " DETERMINISTIC " ); }

        if ( hasDefinersRights())
        { sb.append( " EXTERNAL SECURITY DEFINER " ); }

		sb.append(RoutineAliasInfo.SQL_CONTROL[getSQLAllowed()]);
		if ((returnType == null) &&
			(dynamicResultSets != 0))
		{ // Only print dynamic result sets if this is a PROCEDURE
		  // because it's not valid syntax for FUNCTIONs.
			sb.append(" DYNAMIC RESULT SETS ");
			sb.append(dynamicResultSets);
		}

		if (returnType != null) {
		// this a FUNCTION, so append the syntax telling what to
		// do with a null parameter.
			sb.append(calledOnNullInput ? " CALLED " : " RETURNS NULL ");
			sb.append("ON NULL INPUT");
		}
		
		return sb.toString();
	}

	public static String parameterMode(int parameterMode) {
		switch (parameterMode) {
		case JDBC30Translation.PARAMETER_MODE_IN:
			return "IN";
		case JDBC30Translation.PARAMETER_MODE_OUT:
			return "OUT";
		case JDBC30Translation.PARAMETER_MODE_IN_OUT:
			return "INOUT";
		default:
			return "UNKNOWN";
		}
	}
    
    /**
     * Set the collation type of all string types declared for
     * use in this routine to the given collation type.
     * @param collationType
     */
    public void setCollationTypeForAllStringTypes(int collationType)
    {
        if (parameterCount != 0)
        {
            for (int p = 0; p < parameterTypes.length; p++)
                parameterTypes[p] = DataTypeDescriptor.getCatalogType(
                        parameterTypes[p], collationType);
        }
        
        if (returnType != null)
            returnType = DataTypeDescriptor.getCatalogType(returnType, collationType);
    }
}
