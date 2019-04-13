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
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.util.StringUtil;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.reference.JDBC30Translation;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.TypeId;
import com.splicemachine.db.impl.jdbc.Util;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.sql.Types;

/**
 * A parameter.  Originally lifted from ParameterValueSet.
 */
public final class GenericParameter implements Externalizable {

    public static final long serialVersionUID = 4L;

    // These defaults match the Network Server/ JCC max precision and
    // The JCC "guessed" scale. They are used as the defaults for
    // Decimal out params.
    private static int DECIMAL_PARAMETER_DEFAULT_PRECISION = 38;
    private static int DECIMAL_PARAMETER_DEFAULT_SCALE = 15;


    /*
    ** The parameter set we are part of
    */
    private GenericParameterValueSet pvs;

    /**
     * * Our value
     */
    private DataValueDescriptor value;

    /**
     * Compile time JDBC type identifier.
     */
    int jdbcTypeId;

    /**
     * Compile time Java class name.
     */
    String declaredClassName;

    /**
     * Mode of the parameter, from ParameterMetaData
     */
    short parameterMode;

    /*
    ** If we are set
    */
    boolean isSet;

    /*
    ** Output parameter values
     */
    private boolean isReturnOutputParameter;

    /**
     * Type that has been registered.
     */
    int registerOutType = Types.NULL;
    /**
     * Scale that has been registered.
     */
    int registerOutScale = -1;

    /**
     * When a decimal output parameter is registered we give it a
     * precision
     */

    int registerOutPrecision = -1;

    /* The type of this parameter within a prepared statement. */
    private DataTypeDescriptor dataType;

    /**
     * Constructor for serialization/deserialization. DO NOT USE
     */
    public GenericParameter() {
    }

    /**
     * Constructor for a Parameter
     *
     * @param pvs                     the parameter set that this is part of
     * @param isReturnOutputParameter true if this is a return output parameter
     */
    GenericParameter(GenericParameterValueSet pvs, boolean isReturnOutputParameter) {
        this.pvs = pvs;
        parameterMode = (this.isReturnOutputParameter = isReturnOutputParameter)
                ? (short) JDBC30Translation.PARAMETER_MODE_OUT : (short) JDBC30Translation.PARAMETER_MODE_IN;
    }

    /**
     * Clone myself.  It is a shallow copy for everything but
     * the underlying data wrapper and its value -- e.g. for
     * everything but the underlying SQLInt and its int.
     *
     * @param pvs the parameter value set
     * @return a new generic parameter.
     */
    public GenericParameter getClone(GenericParameterValueSet pvs) {
        GenericParameter gpClone = new GenericParameter(pvs, isReturnOutputParameter);
        gpClone.value = this.getValue().cloneValue(false);
        gpClone.jdbcTypeId = dataType.getJDBCTypeId();
        gpClone.declaredClassName = dataType.getTypeId().getCorrespondingJavaTypeName();
        gpClone.dataType = dataType;
        gpClone.isSet = true;
        return gpClone;
    }

    /**
     * Set the DataValueDescriptor and type information for this parameter
     */
    void initialize(DataTypeDescriptor dtd) throws StandardException {
        this.value = dtd.getNull();
        this.jdbcTypeId = dtd.getJDBCTypeId();
        this.declaredClassName = dtd.getTypeId().getCorrespondingJavaTypeName();
        this.dataType = dtd;
    }

    /**
     * Clear the parameter, unless it is a return
     * output parameter
     */
    void clear() {
        isSet = false;
    }

    /**
     * Get the parameter value.  Doesn't check to see if it has been initialized or not.
     *
     * @return the parameter value, may return null
     */
    DataValueDescriptor getValue() {
        padValueIfTypeIsSQLChar();
        return value;
    }

    /* See DB-3367 and DB-3201.  Pad value to match hbase storage so that we generate the correct scan
     * start/stop keys. */
    private void padValueIfTypeIsSQLChar() {
        try {
            if (dataType != null && dataType.getTypeId().isFixedStringTypeId() && value != null && !value.isNull()) {
                String paramStringVal = value.getString();
                int typeWidth = dataType.getMaximumWidth();
                if (paramStringVal.length() < typeWidth) {
                    value.setValue(StringUtil.padRight(paramStringVal, SQLChar.PAD, typeWidth));
                }
            }
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    void setParameterValueSet(GenericParameterValueSet valueSet) {
        this.pvs = valueSet;
    }

    //////////////////////////////////////////////////////////////////
    //
    // CALLABLE STATEMENT
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Mark the parameter as an output parameter.
     *
     * @param sqlType A type from java.sql.Types
     * @param scale   scale, -1 if no scale arg
     * @throws StandardException on error
     */
    void setOutParameter(int sqlType, int scale) throws StandardException {
        // fast case duplicate registrations.
        if (registerOutType == sqlType) {
            if (scale == registerOutScale)
                return;
        }

        switch (parameterMode) {
            case JDBC30Translation.PARAMETER_MODE_IN:
            case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
            default:
                throw StandardException.newException(SQLState.LANG_NOT_OUT_PARAM, getJDBCParameterNumberStr());

            case JDBC30Translation.PARAMETER_MODE_IN_OUT:
            case JDBC30Translation.PARAMETER_MODE_OUT:
                // Declared/Java procedure parameter.
                if (!DataTypeDescriptor.isJDBCTypeEquivalent(jdbcTypeId, sqlType))
                    throw throwInvalidOutParamMap(sqlType);
                break;

        }

        registerOutType = sqlType;

    }

    private StandardException throwInvalidOutParamMap(int sqlType) {

        //TypeId typeId = TypeId.getBuiltInTypeId(sqlType);
        // String sqlTypeName = typeId == null ? "OTHER" : typeId.getSQLTypeName();


        String jdbcTypesName = Util.typeName(sqlType);

        TypeId typeId = TypeId.getBuiltInTypeId(jdbcTypeId);
        String thisTypeName = typeId == null ? declaredClassName : typeId.getSQLTypeName();

        return StandardException.newException(SQLState.LANG_INVALID_OUT_PARAM_MAP,
                getJDBCParameterNumberStr(),
                jdbcTypesName, thisTypeName);
    }


    /**
     * Validate the parameters.  This is done for situations where
     * we cannot validate everything in the setXXX() calls.  In
     * particular, before we do an execute() on a CallableStatement,
     * we need to go through the parameters and make sure that
     * all parameters are set up properly.  The motivator for this
     * is that setXXX() can be called either before or after
     * registerOutputParamter(), we cannot be sure we have the types
     * correct until we get to execute().
     *
     * @throws StandardException if the parameters aren't valid
     */
    void validate() throws StandardException {
        switch (parameterMode) {
            case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
                break;
            case JDBC30Translation.PARAMETER_MODE_IN:
                break;
            case JDBC30Translation.PARAMETER_MODE_IN_OUT:
            case JDBC30Translation.PARAMETER_MODE_OUT:
                if (registerOutType == Types.NULL) {
                    throw StandardException.newException(SQLState.NEED_TO_REGISTER_PARAM,
                            getJDBCParameterNumberStr(),
                            com.splicemachine.db.catalog.types.RoutineAliasInfo.parameterMode(parameterMode));
                }
                break;
        }
    }

    /**
     * Return the scale of the parameter.
     *
     * @return scale
     */
    int getScale() {
        //when the user doesn't pass any scale, the registerOutScale gets set to -1
        return (registerOutScale == -1 ? 0 : registerOutScale);
    }

    int getPrecision() {
        return registerOutPrecision;

    }

    ////////////////////////////////////////////////////
    //
    // CLASS IMPLEMENTATION
    //
    ////////////////////////////////////////////////////

    /**
     * get string for param number
     */
    String getJDBCParameterNumberStr() {
        return Integer.toString(pvs.getParameterNumber(this));
    }

    @Override
    public String toString() {
        /* This method is used for debugging.
         * It is called when derby.language.logStatementText=true,
         * so there is no check of SanityManager.DEBUG.
         * Anyway, we need to call value.getString() instead of
         * value.toString() because the user may have done a
         * a setStream() on the parameter.  (toString() could get
         * an assertion failure in that case as it would be in an
         * unexpected state since this is a very weird codepath.)
         * getString() can throw an exception which we eat and
         * and reflect in the returned string.
         */
        if (value == null) {
            return "null";
        } else {
            try {
                return value.getTraceString();
            } catch (StandardException se) {
                return "unexpected exception from getTraceString() - " + se;
            }
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.jdbcTypeId);
        out.writeUTF(this.declaredClassName);
        out.writeShort(this.parameterMode);
        out.writeBoolean(this.isSet);
        out.writeBoolean(this.isReturnOutputParameter);
        out.writeInt(this.registerOutType);
        out.writeInt(this.registerOutScale);
        out.writeInt(this.registerOutPrecision);
        out.writeBoolean(!value.isNull());
        if (!value.isNull())
            out.writeObject(this.value);

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        jdbcTypeId = in.readInt();
        declaredClassName = in.readUTF();
        parameterMode = in.readShort();
        isSet = in.readBoolean();
        isReturnOutputParameter = in.readBoolean();
        registerOutType = in.readInt();
        registerOutScale = in.readInt();
        registerOutPrecision = in.readInt();
        if (in.readBoolean())
            value = (DataValueDescriptor) in.readObject();
        else {
            try {
                value = DataTypeDescriptor.getBuiltInDataTypeDescriptor(jdbcTypeId).getNull();
            } catch (StandardException se) {
                throw new IOException(se);
            }
        }
    }
}
