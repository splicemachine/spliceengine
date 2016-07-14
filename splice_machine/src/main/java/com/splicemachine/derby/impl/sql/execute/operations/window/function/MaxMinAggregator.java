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

package com.splicemachine.derby.impl.sql.execute.operations.window.function;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;

/**
 *
 * Created by jyuan on 7/22/14.
 */
public class MaxMinAggregator extends SpliceGenericWindowFunction {
    private boolean isMax;

    @Override
    public WindowFunction setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                 FormatableHashtable functionSpecificArgs) {
        super.setup( cf, aggregateName, returnType );
        isMax = aggregateName.equals("MAX");
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvd) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            chunk.setResult(dvd[0]);
        }
        else if(isMax && dvd[0].compare(result) > 0) {
            chunk.setResult(dvd[0]);
        }
        else if (!isMax && dvd[0].compare(result) < 0) {
            chunk.setResult(dvd[0]);
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (dvds[0].compare(result) == 0) {
            recalculate(chunk);
        }
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
        DataValueDescriptor result = chunk.get(chunk.first)[0];

        for (int i = chunk.first+1; i < chunk.last; ++i) {
            DataValueDescriptor dvd = chunk.get(i)[0];
            if (isMax) {
                if (dvd.compare(result) > 0) {
                    result = dvd;
                }
            }
            else {
                if (dvd.compare(result) < 0) {
                    result = dvd;
                }
            }
        }
        chunk.setResult(result);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        WindowChunk first = chunks.get(0);
        DataValueDescriptor result = first.getResult();
        for (int i = 1; i < chunks.size(); ++i) {
            DataValueDescriptor dvd = chunks.get(i).getResult();
            if (isMax) {
                if (dvd.compare(result) > 0) {
                    result = dvd;
                }
            }
            else {
                if (dvd.compare(result) < 0) {
                    result = dvd;
                }
            }
        }
        return result;
    }

    @Override
    public WindowFunction newWindowFunction() {
        MaxMinAggregator ma = new MaxMinAggregator();
        ma.isMax = isMax;
        return ma;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeBoolean(isMax);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        isMax = in.readBoolean();
    }
}
