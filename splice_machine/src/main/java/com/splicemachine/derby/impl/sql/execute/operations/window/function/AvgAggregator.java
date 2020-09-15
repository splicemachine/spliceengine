/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.NumberDataValue;

/**
 *
 * Created by jyuan on 7/31/14.
 */
public class AvgAggregator extends SpliceGenericWindowFunction {

    @Override
    public WindowFunction setup( ClassFactory cf, String aggregateName, DataTypeDescriptor returnType,
                                 FormatableHashtable functionSpecificArgs) {
        super.setup( cf, aggregateName, returnType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            chunk.setResult(dvds[0].cloneValue(false));
        } else {
            NumberDataValue input = (NumberDataValue)dvds[0];
            if (input != null && ! input.isNull()) {
                NumberDataValue nv = (NumberDataValue) result.cloneValue(false);
                nv.plus(input, nv, nv);
                chunk.setResult(nv);
            }
        }
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        NumberDataValue input = (NumberDataValue)dvds[0];
        if (input != null && ! input.isNull()) {
            NumberDataValue nv = (NumberDataValue) result.cloneValue(false);
            nv.minus(nv, input, nv);
            chunk.setResult(nv);
        }
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        if (chunks.isEmpty() || chunks.get(0).isEmpty())
            return null;
        WindowChunk first = chunks.get(0);
        long count = first.last - first.first;
        NumberDataValue result = (NumberDataValue)first.getResult().cloneValue(false);
        long r = result.getLong();
        for (int i = 1; i < chunks.size(); ++i) {
            WindowChunk chunk = chunks.get(i);
            NumberDataValue dvd = (NumberDataValue)chunk.getResult();
            r += dvd.getLong();
            count += chunk.last - chunk.first;
        }
        result.setValue((r*1.0)/count);
        return result;
    }

    public WindowFunction newWindowFunction() {
        return new AvgAggregator();
    }
}
