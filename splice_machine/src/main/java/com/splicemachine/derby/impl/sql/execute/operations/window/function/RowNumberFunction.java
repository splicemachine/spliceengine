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
import com.splicemachine.db.iapi.types.SQLLongint;

/**
 * Implementation of ROW_NUMBER -  Assigns a sequential number to each row in partition.
 *
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class RowNumberFunction extends SpliceGenericWindowFunction implements WindowFunction {
    private long rowNum;

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup(classFactory, aggregateName, returnDataType);
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    public void reset() {
        super.reset();
        rowNum = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        // row number is always increasing increasing as we iterate thru the window
        rowNum++;
        // always collect the now previous value
        chunk.setPrevious(dvds);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // just return the current rowNum
        return new SQLLongint(rowNum);
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new RowNumberFunction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(rowNum);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowNum = in.readLong();
    }
}
