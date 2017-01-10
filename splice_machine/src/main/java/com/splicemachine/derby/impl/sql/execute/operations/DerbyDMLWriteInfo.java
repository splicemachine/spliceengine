/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.impl.sql.execute.actions.WriteCursorConstantOperation;
import com.splicemachine.derby.impl.sql.execute.operations.iapi.DMLWriteInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.sql.execute.ConstantAction;
import com.splicemachine.si.api.txn.Txn;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 10/4/13
 */
public class DerbyDMLWriteInfo implements DMLWriteInfo {
    private transient Activation activation;
    private Txn txn;

    @Override
    public void initialize(SpliceOperationContext opCtx) throws StandardException {
        this.activation = opCtx.getActivation();
        this.txn = opCtx.getTxn();
    }

    @Override
    public ConstantAction getConstantAction() {
        return activation.getConstantAction();
    }

    @Override
    public FormatableBitSet getPkColumns() {
        return fromIntArray(getPkColumnMap());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public int[] getPkColumnMap() {
        ConstantAction action = getConstantAction();
        return ((WriteCursorConstantOperation)action).getPkColumns();
    }

    @Override
    public long getConglomerateId() {
        return ((WriteCursorConstantOperation)getConstantAction()).getConglomerateId();
    }

    @Override
		public ResultDescription getResultDescription() {
				return activation.getResultDescription();
		}

    public static FormatableBitSet fromIntArray(int[] values){
        if(values ==null) return null;
        FormatableBitSet fbt = new FormatableBitSet(values.length);
        for(int value:values){
            fbt.grow(value);
            fbt.set(value-1);
        }
        return fbt;
    }
}
