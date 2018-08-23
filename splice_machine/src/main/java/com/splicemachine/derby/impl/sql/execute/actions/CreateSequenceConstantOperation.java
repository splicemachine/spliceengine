/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.SchemaDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.SequenceDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.DataDescriptorGenerator;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.shared.common.reference.SQLState;
import org.apache.log4j.Logger;

import com.splicemachine.utils.SpliceLogUtils;

/**
 * This class performs actions that are ALWAYS performed for a
 * CREATE SEQUENCE statement at execution time.
 * These SQL objects are stored in the SYS.SYSSEQUENCES table.
 */
public class CreateSequenceConstantOperation extends DDLConstantOperation {
	private static final Logger LOG = Logger.getLogger(CreateSequenceConstantOperation.class);
    private String _sequenceName;
    private String _schemaName;
    private DataTypeDescriptor _dataType;
    private long _initialValue;
    private long _stepValue;
    private long _maxValue;
    private long _minValue;
    private boolean _cycle;

    /**
     * Make the ConstantAction for a CREATE SEQUENCE statement.
     * When executed, will create a sequence by the given name.
     *
     * @param sequenceName The name of the sequence being created
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     */
    public CreateSequenceConstantOperation (String schemaName, String sequenceName, 
    		DataTypeDescriptor dataType, long initialValue, long stepValue,
            long maxValue, long minValue, boolean cycle) {
    	SpliceLogUtils.trace(LOG, "CreateSequenceConstantOperation %s.%s for data type {%s} with initialValue" +
    			"%d, step value %d, maxValue %d, minValue %d and cycle %s",schemaName, sequenceName,
    			dataType, initialValue, stepValue, maxValue, minValue, cycle);
        this._schemaName = schemaName;
        this._sequenceName = sequenceName;
        this._dataType = dataType;
        this._initialValue = initialValue;
        this._stepValue = stepValue;
        this._maxValue = maxValue;
        this._minValue = minValue;
        this._cycle = cycle;
    }

    /**
     * This is the guts of the Execution-time logic for CREATE SEQUENCE.
     *
     * @throws com.splicemachine.db.iapi.error.StandardException
     *          Thrown on failure
     * @see com.splicemachine.db.iapi.sql.execute.ConstantAction#executeConstantAction
     */
    @Override
    public void executeConstantAction(Activation activation) throws StandardException {
    	SpliceLogUtils.trace(LOG, "executeConstantAction");
        SchemaDescriptor schemaDescriptor;
        LanguageConnectionContext lcc =
                activation.getLanguageConnectionContext();
        DataDictionary dd = lcc.getDataDictionary();
        TransactionController tc = lcc.getTransactionExecute();
        DataDescriptorGenerator ddg = dd.getDataDescriptorGenerator();

        dd.startWriting(lcc);

        schemaDescriptor = DDLConstantOperation.getSchemaDescriptorForCreate(dd, activation, _schemaName);

        //
        // Check if this sequence already exists. If it does, throw.
        //
        SequenceDescriptor seqDef = dd.getSequenceDescriptor(schemaDescriptor, _sequenceName);

        if (seqDef != null) {
            throw StandardException.
                    newException(SQLState.LANG_OBJECT_ALREADY_EXISTS,
                            seqDef.getDescriptorType(), _sequenceName);
        }

        seqDef = ddg.newSequenceDescriptor(
                schemaDescriptor,
                dd.getUUIDFactory().createUUID(),
                _sequenceName,
                _dataType,
                _initialValue,   // current value
                _initialValue,
                _minValue,
                _maxValue,
                _stepValue,
                _cycle);        // whether the sequence can wrap-around

        dd.addDescriptor(seqDef,
                null,  // parent
                DataDictionary.SYSSEQUENCES_CATALOG_NUM,
                false, // duplicatesAllowed
                tc);
    }

    public String toString() {
        return "CREATE SEQUENCE " + _sequenceName;
    }
}

