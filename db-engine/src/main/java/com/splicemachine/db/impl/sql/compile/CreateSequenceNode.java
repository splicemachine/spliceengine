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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.types.TypeId;


/**
 * A CreateSequenceNode is the root of a QueryTree that
 * represents a CREATE SEQUENCE statement.
 */

public class CreateSequenceNode extends DDLStatementNode
{
    private TableName _sequenceName;
    private DataTypeDescriptor _dataType;
    private Long _initialValue;
    private Long _stepValue;
    private Long _maxValue;
    private Long _minValue;
    private Boolean _cycle;

    public static final int SEQUENCE_ELEMENT_COUNT = 1;

    /**
     * Initializer for a CreateSequenceNode
     *
     * @param sequenceName The name of the new sequence
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     *
     * @throws com.splicemachine.db.iapi.error.StandardException on error
     */
    public void init
        (
         Object sequenceName,
         Object dataType,
         Object initialValue,
         Object stepValue,
         Object maxValue,
         Object minValue,
         Object cycle
         ) throws StandardException {

        _sequenceName = (TableName) sequenceName;
        initAndCheck(_sequenceName);

        if (dataType != null) {
            _dataType = (DataTypeDescriptor) dataType;
        } else {
            _dataType = DataTypeDescriptor.INTEGER;
        }

        _stepValue = (stepValue != null ? (Long) stepValue : new Long(1));

        if (_dataType.getTypeId().equals(TypeId.SMALLINT_ID)) {
            _minValue = (minValue != null ? (Long) minValue : new Long(Short.MIN_VALUE));
            _maxValue = (maxValue != null ? (Long) maxValue : new Long(Short.MAX_VALUE));
        } else if (_dataType.getTypeId().equals(TypeId.INTEGER_ID)) {
            _minValue = (minValue != null ? (Long) minValue : new Long(Integer.MIN_VALUE));
            _maxValue = (maxValue != null ? (Long) maxValue : new Long(Integer.MAX_VALUE));
        } else {
            // Could only be BIGINT
            _minValue = (minValue != null ? (Long) minValue : new Long(Long.MIN_VALUE));
            _maxValue = (maxValue != null ? (Long) maxValue : new Long(Long.MAX_VALUE));
        }

        if (initialValue != null) {
            _initialValue = (Long) initialValue;
        } else {
            if (_stepValue > 0L) {
                _initialValue = _minValue;
            } else {
                _initialValue = _maxValue;
            }
        }
        _cycle = (cycle != null ? (Boolean) cycle : Boolean.FALSE);

        // automatically create the schema if it doesn't exist
        implicitCreateSchema = true;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        if (SanityManager.DEBUG) {
            return super.toString() +
                    "sequenceName: " + "\n" + _sequenceName + "\n";
        } else {
            return "";
        }
    }

    /**
     * Bind this CreateSequenceNode.
     * The main objectives of this method are to resolve the schema name, determine privilege checks,
     * and vet the variables in the CREATE SEQUENCE statement.
     */
    public void bindStatement() throws StandardException {
        CompilerContext cc = getCompilerContext();

        // implicitly create the schema if it does not exist.
        // this method also compiles permissions checks
        SchemaDescriptor sd = getSchemaDescriptor();

        // set the default schema name if the user did not explicitly specify a schema
        if (_sequenceName.getSchemaName() == null) {
            _sequenceName.setSchemaName(sd.getSchemaName());
        }

        if (_dataType.getTypeId().equals(TypeId.SMALLINT_ID)) {
            if (_minValue < Short.MIN_VALUE || _minValue >= Short.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "SMALLINT",
                        Short.MIN_VALUE + "",
                        Short.MAX_VALUE + "");
            }
            if (_maxValue <= Short.MIN_VALUE || _maxValue > Short.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "SMALLINT",
                        Short.MIN_VALUE + "",
                        Short.MAX_VALUE + "");
            }
        } else if (_dataType.getTypeId().equals(TypeId.INTEGER_ID)) {
            if (_minValue < Integer.MIN_VALUE || _minValue >= Integer.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "INTEGER",
                        Integer.MIN_VALUE + "",
                        Integer.MAX_VALUE + "");
            }
            if (_maxValue <= Integer.MIN_VALUE || _maxValue > Integer.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "INTEGER",
                        Integer.MIN_VALUE + "",
                        Integer.MAX_VALUE + "");
            }
        } else {
            // BIGINT
            if (_minValue < Long.MIN_VALUE || _minValue >= Long.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MINVALUE",
                        "BIGINT",
                        Long.MIN_VALUE + "",
                        Long.MAX_VALUE + "");
            }
            if (_maxValue <= Long.MIN_VALUE || _maxValue > Long.MAX_VALUE) {
                throw StandardException.newException(
                        SQLState.LANG_SEQ_ARG_OUT_OF_DATATYPE_RANGE,
                        "MAXVALUE",
                        "BIGINT",
                        Long.MIN_VALUE + "",
                        Long.MAX_VALUE + "");
            }
        }

        if (_minValue >= _maxValue) {
            throw StandardException.newException(
                    SQLState.LANG_SEQ_MIN_EXCEEDS_MAX,
                    _minValue.toString(),
                    _maxValue.toString());
        }

        if (_initialValue < _minValue || _initialValue > _maxValue) {
             throw StandardException.newException(
                     SQLState.LANG_SEQ_INVALID_START,
                     _initialValue.toString(),
                     _minValue.toString(),
                     _maxValue.toString());
        }       

        if (_stepValue == 0L) {
            throw StandardException.newException(
                    SQLState.LANG_SEQ_INCREMENT_ZERO);
        }

    }

    public String statementToString() {
        return "CREATE SEQUENCE";
    }

    // We inherit the generate() method from DDLStatementNode.

    /**
     * Create the Constant information that will drive the guts of Execution.
     *
     * @throws com.splicemachine.db.iapi.error.StandardException
     *          Thrown on failure
     */
    public ConstantAction makeConstantAction() {
             return getGenericConstantActionFactory().
                getCreateSequenceConstantAction(
                        _sequenceName,
                        _dataType,
                        _initialValue,
                        _stepValue,
                        _maxValue,
                        _minValue,
                        _cycle);
    }


}
