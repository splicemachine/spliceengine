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
 * Implementation of DENSE_RANK -  Densely ranks each row in a partition.
 * <p/>
 * If values in the ranking columns (ORDER BY) are the same, they receive the same rank. Conversely,
 * when the ranking columns for the current row differ from the ranking columns in the previous row
 * the current row's rank increases.  That is, dense rank will have a sequence like
 * <code>1, 2, 3, 3, 3, 4, 5,...</code>
 * <p/>
 * Note that, since the rows coming to us have already been sorted by their ranking columns, we don't
 * need to compare to the point of determining order.  We just need to see if the ranking columns
 * differ.
 *
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class DenseRankFunction extends SpliceGenericWindowFunction implements WindowFunction {
    private long rank;

    @Override
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType,
                                FormatableHashtable functionSpecificArgs) {
        super.setup( classFactory, aggregateName, returnDataType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor[] valueDescriptors) throws StandardException {
        this.add(valueDescriptors);
    }

    @Override
    public void reset() {
        super.reset();
        rank = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor[] previous = chunk.getPrevious();
        if ( isNullOrEmpty(previous) || compareDVDArrays(previous,dvds) != 0) {
            // if previous result is null or if current and previous values differ, rank increases
            rank++;
        }
        // always collect the now previous value
        chunk.setPrevious(dvds);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor[] previous = chunk.getPrevious();
        if (compareDVDArrays(previous, dvds) == 0) {
            recalculate(chunk);
        }
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // just return the current rank
        return new SQLLongint(rank);
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new DenseRankFunction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

}