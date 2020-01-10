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

package com.splicemachine.db.impl.jdbc;

import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataTypeUtilities;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.jdbc.EngineParameterMetaData;

import java.sql.SQLException;

/**
 * This class immitates to implement the ParameterMetaData interface from JDBC3.0
 * We want to provide the functionality to JDKs before JDBC3.0. We put it here
 * instead of in Local20 because we want to make it available for CallableStatement.
 * It provides the parameter meta data for callable & prepared statements.
 * The subclass in Local30 actually implements ParameterMetaData interface.
 *
 * For use of ParameterMetaData functionality in network server, please do not use
 * this class directly. Instead use the method available on EnginePreparedStatement
 * @see com.splicemachine.db.iapi.jdbc.EngineParameterMetaData
 * @see com.splicemachine.db.iapi.jdbc.EnginePreparedStatement
 */
public class EmbedParameterSetMetaData implements EngineParameterMetaData
    {

    private final ParameterValueSet pvs;
    private final DataTypeDescriptor[] types;
    private final int paramCount;

	//////////////////////////////////////////////////////////////
	//
	// CONSTRUCTORS
	//
	//////////////////////////////////////////////////////////////
    protected EmbedParameterSetMetaData(ParameterValueSet pvs, DataTypeDescriptor[] types)  {
		int paramCount;
		paramCount = pvs.getParameterCount();
		this.pvs = pvs;
		this.paramCount = paramCount;
		this.types = types;
	}
	/**
    *
    * Retrieves the number of parameters in the PreparedStatement object for which
    * this ParameterMetaData object contains information.
    *
    * @return the number of parameters
    */
    public int getParameterCount() {
   		return paramCount;
    }

	/**
    *
    * Retrieves whether null values are allowed in the designated parameter.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return the nullability status of the given parameter; one of
    * ParameterMetaData.parameterNoNulls, ParameterMetaData.parameterNullable, or
    * ParameterMetaData.parameterNullableUnknown
    * @exception SQLException if a database access error occurs
    */
    public int isNullable(int param) throws SQLException {
   		checkPosition(param);

   		if (types[param - 1].isNullable())
			return JDBC30Translation.PARAMETER_NULLABLE;
   		else
			return JDBC30Translation.PARAMETER_NO_NULLS;
    }

	/**
    *
    * Retrieves whether values for the designated parameter can be signed numbers.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return true if it can be signed numbers
    * @exception SQLException if a database access error occurs
    */
    public boolean isSigned(int param) throws SQLException {
   		checkPosition(param);

   		return types[param - 1].getTypeId().isNumericTypeId();
    }

	/**
    *
    * Retrieves the designated parameter's number of decimal digits.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return precision
    * @exception SQLException if a database access error occurs
    */
    public int getPrecision(int param) throws SQLException {
   		checkPosition(param);

		int outparamPrecision = -1;
	   
   		if (((param == 1) && pvs.hasReturnOutputParameter()))
		{
			outparamPrecision = pvs.getPrecision(param);
		}

        if (outparamPrecision == -1)
        {
            return DataTypeUtilities.getPrecision(types[param - 1]);
        }

		return outparamPrecision;
    }
		
	/**
    *
    * Retrieves the designated parameter's number of digits to right of the decimal point.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return scale
    * @exception SQLException if a database access error occurs
    */
    public int getScale(int param) throws SQLException {
   		checkPosition(param);

		if (((param == 1) && pvs.hasReturnOutputParameter()))
			return pvs.getScale(param);
   		return types[param - 1].getScale();

    }

	/**
    *
    * Retrieves the designated parameter's SQL type.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return SQL type from java.sql.Types
    * @exception SQLException if a database access error occurs
    */
    public int getParameterType(int param) throws SQLException {
   		checkPosition(param);

   		return types[param - 1].getTypeId().getJDBCTypeId();
    }

	/**
    *
    * Retrieves the designated parameter's database-specific type name.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return type the name used by the database. If the parameter
    * type is a user-defined type, then a fully-qualified type name is returned.
    * @exception SQLException if a database access error occurs
    */
    public String getParameterTypeName(int param) throws SQLException {
   		checkPosition(param);

   		return types[param - 1].getTypeId().getSQLTypeName();
    }

	/**
    *
    * Retrieves the fully-qualified name of the Java class whose instances should be
    * passed to the method PreparedStatement.setObject.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return the fully-qualified name of the class in the Java
    * programming language that would be used by the method
    * PreparedStatement.setObject to set the value in the specified parameter.
    * This is the class name used for custom mapping.
    * @exception SQLException if a database access error occurs
    */
    public String getParameterClassName(int param) throws SQLException {
   		checkPosition(param);

   		return types[param - 1].getTypeId().getResultSetMetaDataTypeName();
    }

	/**
    *
    * Retrieves the designated parameter's mode.
    *
    * @param param - the first parameter is 1, the second is 2, ...
    * @return mode of the parameter; one of ParameterMetaData.parameterModeIn,
    * ParameterMetaData.parameterModeOut, or ParameterMetaData.parameterModeInOut
    * ParameterMetaData.parameterModeUnknown.
    * @exception SQLException if a database access error occurs
    */
    public int getParameterMode(int param) throws SQLException {
   		checkPosition(param);

   		//bug 4857 - only the return parameter is of type OUT. All the other output
   		//parameter are IN_OUT (it doesn't matter if their value is set or not).
   		if ((param == 1) && pvs.hasReturnOutputParameter())//only the first parameter can be of return type
				return JDBC30Translation.PARAMETER_MODE_OUT;
   		return pvs.getParameterMode(param);
    }


    // Check the position number for a parameter and throw an exception if
    // it is out of range.
    private void checkPosition(int parameterIndex) throws SQLException {
   		/* Check that the parameterIndex is in range. */
   		if (parameterIndex < 1 ||
				parameterIndex > paramCount) {

			/* This message matches the one used by the DBMS */
			throw Util.generateCsSQLException(
            SQLState.LANG_INVALID_PARAM_POSITION,
                    parameterIndex, paramCount);
		}
    }
}

