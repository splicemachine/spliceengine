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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableHashtable;
import com.splicemachine.db.iapi.services.loader.ClassFactory;
import com.splicemachine.db.iapi.sql.execute.WindowFunction;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLLongint;

/**
 *
 * Created by jyuan on 7/31/14.
 */
public class CountAggregator extends SpliceGenericWindowFunction {

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
    protected void calculateOnAdd(SpliceGenericWindowFunction.WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException{
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            SQLLongint r = new SQLLongint(1);
            chunk.setResult(r);
        } else {
            long count = result.getLong();
            result.setValue(count+1);
            chunk.setResult(result);
        }
    }

    @Override
    protected void calculateOnRemove(SpliceGenericWindowFunction.WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        long count = result.getLong();
        result.setValue(count-1);
        chunk.setResult(result);
    }

    public DataValueDescriptor getResult() throws StandardException {
        // Iterate through each chunk, compute the max/min of each chunk
        long count = 0;
        for (WindowChunk chunk : chunks) {
            count += chunk.getResult().getLong();
        }
        return new SQLLongint(count);
    }

    public WindowFunction newWindowFunction() {
        return new CountAggregator();
    }
}
