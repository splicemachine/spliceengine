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

package com.splicemachine.db.impl.sql;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.services.loader.ClassInspector;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.reference.JDBC30Translation;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.ParameterValueSet;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.UserDataValue;

import java.io.*;
import java.sql.Types;

/**
 * Implementation of ParameterValueSet
 *
 * @see ParameterValueSet
 */

public final class GenericParameterValueSet implements ParameterValueSet, Externalizable {

    public static final long serialVerionUID = 1l;

    //all this has to be copied in the clone constructor
    private GenericParameter[] parms;
    private ClassInspector ci;
    private boolean hasReturnOutputParam;

    /*
     * Constructor for serialization/deserialization, DO NOT USE
     */
    public GenericParameterValueSet() {
    }

    /**
     * Constructor for a GenericParameterValueSet
     *
     * @param numParms             The number of parameters in the new ParameterValueSet
     * @param hasReturnOutputParam if we have a ? = call syntax.  Note that
     *                             this is NOT the same thing as an output parameter -- return
     *                             output parameters are special cases of output parameters.
     */
    GenericParameterValueSet(ClassInspector ci, int numParms, boolean hasReturnOutputParam) {
        this.ci = ci;
        this.hasReturnOutputParam = hasReturnOutputParam;
        parms = new GenericParameter[numParms];
        for (int i = 0; i < numParms; i++) {
            /*
            ** Last param is if this is a return output param.  True if
            ** we have an output param and we are on the 1st parameter.
            */
            parms[i] = new GenericParameter(this, (hasReturnOutputParam && i == 0));
        }
    }

    /*
    ** Construct a pvs by cloning a pvs.
    */
    private GenericParameterValueSet(int numParms, GenericParameterValueSet pvs) {
        this.hasReturnOutputParam = pvs.hasReturnOutputParam;
        this.ci = pvs.ci;
        parms = new GenericParameter[numParms];
        for (int i = 0; i < numParms; i++) {
            parms[i] = pvs.getGenericParameter(i).getClone(this);
        }
    }

    /*
    ** ParameterValueSet interface methods
    */

    /**
     * Initialize the set by allocating a holder DataValueDescriptor object
     * for each parameter.
     */
    @Override
    public void initialize(DataTypeDescriptor[] types) throws StandardException {
        for (int i = 0; i < parms.length; i++) {
            parms[i].initialize(types[i]);
        }
    }

    @Override
    public void setParameterMode(int position, int mode) {
        parms[position].parameterMode = (short) mode;
    }

    @Override
    public void clearParameters() {
        for (GenericParameter parm : parms) {
            parm.clear();
        }
    }

    /**
     * Returns the number of parameters in this set.
     */
    @Override
    public int getParameterCount() {
        return parms.length;
    }

    /**
     * Returns the parameter value at the given position.
     *
     * @return The parameter at the given position.
     */
    @Override
    public DataValueDescriptor getParameter(int position) throws StandardException {
        try {
            return parms[position].getValue();
        } catch (ArrayIndexOutOfBoundsException e) {
            checkPosition(position);
            return null;
        }
    }


    @Override
    public DataValueDescriptor getParameterForSet(int position) throws StandardException {
        try {
            GenericParameter gp = parms[position];
            if (gp.parameterMode == JDBC30Translation.PARAMETER_MODE_OUT)
                throw StandardException.newException(SQLState.LANG_RETURN_OUTPUT_PARAM_CANNOT_BE_SET);

            gp.isSet = true;

            return gp.getValue();
        } catch (ArrayIndexOutOfBoundsException e) {
            checkPosition(position);
            return null;
        }

    }

    @Override
    public DataValueDescriptor getParameterForGet(int position) throws StandardException {
        try {
            GenericParameter gp = parms[position];
            switch (gp.parameterMode) {
                case JDBC30Translation.PARAMETER_MODE_IN:
                case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
                    throw StandardException.newException(SQLState.LANG_NOT_OUTPUT_PARAMETER, Integer.toString(position + 1));
            }

            return gp.getValue();
        } catch (ArrayIndexOutOfBoundsException e) {
            checkPosition(position);
            return null;
        }
    }

    @Override
    public void setParameterAsObject(int position, Object value) throws StandardException {
        UserDataValue dvd = (UserDataValue) getParameterForSet(position);
        GenericParameter gp = parms[position];
        if (value != null) {

            {

                boolean throwError;
                ClassNotFoundException t = null;
                try {
                    throwError = !ci.instanceOf(gp.declaredClassName, value);
                } catch (ClassNotFoundException cnfe) {
                    t = cnfe;
                    throwError = true;
                }

                if (throwError) {
                    throw StandardException.newException(SQLState.LANG_DATA_TYPE_SET_MISMATCH, t,
                            ClassInspector.readableClassName(value.getClass()), gp.declaredClassName);
                }
            }

        }
        dvd.setValue(value);
    }

    @Override
    public boolean allAreSet() {
        for (GenericParameter gp : parms) {
            if (!gp.isSet) {
                switch (gp.parameterMode) {
                    case JDBC30Translation.PARAMETER_MODE_OUT:
                        break;
                    case JDBC30Translation.PARAMETER_MODE_IN_OUT:
                    case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
                    case JDBC30Translation.PARAMETER_MODE_IN:
                        return false;
                }
            }
        }

        return true;
    }

    @Override
    public void transferDataValues(ParameterValueSet pvstarget) throws StandardException {
        // don't take application's values for return output parameters
        int firstParam = pvstarget.hasReturnOutputParameter() ? 1 : 0;
        for (int i = firstParam; i < parms.length; i++) {

            GenericParameter oldp = parms[i];

            if (oldp.registerOutType != Types.NULL) {

                pvstarget.registerOutParameter(i, oldp.registerOutType, oldp.registerOutScale);

            }

            if (oldp.isSet) {
                DataValueDescriptor dvd = oldp.getValue();
                InputStream is = null;
                // See if the value type can hold a stream.
                if (dvd.hasStream()) {
                    // DERBY-4455: Don't materialize the stream when
                    // transferring it. If the stream has been drained already,
                    // and the user doesn't set a new value before executing
                    // the prepared statement again, Derby will fail.
                    pvstarget.getParameterForSet(i).setValue(dvd.getStream(),
                            DataValueDescriptor.UNKNOWN_LOGICAL_LENGTH);
                } else {
                    pvstarget.getParameterForSet(i).setValue(dvd);
                }
            }
        }
    }

    GenericParameter getGenericParameter(int position) {
        return (parms[position]);
    }

    @Override
    public String toString() {
        /* This method needed for derby.language.logStatementText=true.
         * Do not put under SanityManager.DEBUG.
         */
        StringBuilder strbuf = new StringBuilder();

        for (int ctr = 0; ctr < parms.length; ctr++) {
            strbuf.append("begin parameter #" + (ctr + 1) + ": ");
            strbuf.append(parms[ctr].toString());
            strbuf.append(" :end parameter ");
        }

        return strbuf.toString();
    }

    /**
     * Check the position number for a parameter and throw an exception if
     * it is out of range.
     *
     * @param position The position number to check
     * @throws StandardException Thrown if position number is
     *                           out of range.
     */
    private void checkPosition(int position) throws StandardException {
        if (position < 0 || position >= parms.length) {

            if (parms.length == 0)
                throw StandardException.newException(SQLState.NO_INPUT_PARAMETERS);

            throw StandardException.newException(SQLState.LANG_INVALID_PARAM_POSITION,
                    String.valueOf(position + 1),
                    String.valueOf(parms.length));
        }
    }


    @Override
    public ParameterValueSet getClone() {
        return (new GenericParameterValueSet(parms.length, this));
    }

    //////////////////////////////////////////////////////////////////
    //
    // CALLABLE STATEMENT
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Mark the parameter as an output parameter.
     *
     * @param parameterIndex The ordinal parameterIndex of a parameter to set
     *                       to the given value.
     * @param sqlType        A type from java.sql.Types
     * @param scale          the scale to use.  -1 means ignore scale
     * @throws StandardException on error
     */
    @Override
    public void registerOutParameter(int parameterIndex, int sqlType, int scale) throws StandardException {
        checkPosition(parameterIndex);
        parms[parameterIndex].setOutParameter(sqlType, scale);
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
    @Override
    public void validate() throws StandardException {
        for (GenericParameter parm : parms) {
            parm.validate();
        }
    }


    /**
     * Return the parameter number (in jdbc lingo, i.e. 1 based) for the given parameter.  Linear search.
     *
     * @return the parameter number, or 0 if not found
     */
    public int getParameterNumber(GenericParameter theParam) {
        for (int i = 0; i < parms.length; i++) {
            if (parms[i] == theParam) {
                return i + 1;
            }
        }
        return 0;
    }

    /**
     * Check that there are not output parameters defined
     * by the parameter set. If there are unknown parameter
     * types they are forced to input types. i.e. Derby static method
     * calls with parameters that are array.
     *
     * @return true if a declared Java Procedure INOUT or OUT parameter is in the set, false otherwise.
     */
    @Override
    public boolean checkNoDeclaredOutputParameters() {
        boolean hasDeclaredOutputParameter = false;
        for (GenericParameter gp : parms) {
            switch (gp.parameterMode) {
                case JDBC30Translation.PARAMETER_MODE_IN:
                    break;
                case JDBC30Translation.PARAMETER_MODE_IN_OUT:
                case JDBC30Translation.PARAMETER_MODE_OUT:
                    hasDeclaredOutputParameter = true;
                    break;
                case JDBC30Translation.PARAMETER_MODE_UNKNOWN:
                    gp.parameterMode = JDBC30Translation.PARAMETER_MODE_IN;
                    break;
            }
        }
        return hasDeclaredOutputParameter;
    }

    /**
     * Return the mode of the parameter according to JDBC 3.0 ParameterMetaData
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     */
    @Override
    public short getParameterMode(int parameterIndex) {
        //if (mode == (short) JDBC30Translation.PARAMETER_MODE_UNKNOWN)
        //    mode = (short) JDBC30Translation.PARAMETER_MODE_IN;
        return parms[parameterIndex - 1].parameterMode;
    }

    /**
     * Is there a return output parameter in this pvs.  A return
     * parameter is from a CALL statement of the following
     * syntax: ? = CALL myMethod()
     *
     * @return true if it has a return parameter
     */
    @Override
    public boolean hasReturnOutputParameter() {
        return hasReturnOutputParam;
    }

    /**
     * Get the value of the return parameter in order to set it.
     *
     * @throws StandardException if a database-access error occurs.
     */
    @Override
    public DataValueDescriptor getReturnValueForSet() throws StandardException {
        checkPosition(0);
        if (SanityManager.DEBUG) {
            if (!hasReturnOutputParam)
                SanityManager.THROWASSERT("getReturnValueForSet called on non-return parameter");
        }
        return parms[0].getValue();
    }

    /**
     * Return the scale of the given parameter index in this pvs.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return scale
     */
    @Override
    public int getScale(int parameterIndex) {
        return parms[parameterIndex - 1].getScale();
    }

    /**
     * Return the precision of the given parameter index in this pvs.
     *
     * @param parameterIndex the first parameter is 1, the second is 2, ...
     * @return precision
     */
    @Override
    public int getPrecision(int parameterIndex) {
        return parms[parameterIndex - 1].getPrecision();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        /*
         * For the moment, we ignore the ClassInspector, because it's not
         * serializable (and even if it were, it would probably not work the way
         * we wanted it to). It's only used to set parameters, which should be
         * done client-side before serialization anyway, so it shouldn't be a
         * big deal.
         */
        ArrayUtil.writeArray(out, parms);
        out.writeBoolean(hasReturnOutputParam);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        parms = new GenericParameter[in.readInt()];
        ArrayUtil.readArrayItems(in, parms);
        hasReturnOutputParam = in.readBoolean();

        //set the value set on each parameter
        for (GenericParameter parm : parms) {
            parm.setParameterValueSet(this);
        }
    }
}

