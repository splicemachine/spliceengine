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
