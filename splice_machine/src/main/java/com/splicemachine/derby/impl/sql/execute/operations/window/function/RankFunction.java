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
 * Implementation of RANK -  Sparsely ranks each row in a partition.
 * <p/>
 * If values in the ranking columns (ORDER BY) are the same, they receive the same rank. However,
 * when the ranking columns for the current row differ from the ranking columns in the previous row
 * the current row's rank increases but interleaving rank numbers are skipped. That is, sparse
 * rank will have a sequence like <code>1, 2, 3, 3, 3, 6, 7,...</code>
 * <p/>
 * Note that, since the rows coming to us have already been sorted by their ranking columns, we don't
 * need to compare to the point of determining order.  We just need to see if the ranking columns
 * differ.
 *
 * @author Jeff Cunningham
 *         Date: 8/5/14
 */
public class RankFunction extends SpliceGenericWindowFunction implements WindowFunction {
    // running count of all the rows
    private long rowNum;
    // the row rank. If values in the ranking column are the same, they receive the same rank.
    // If not, the next number(s) in the ranking sequence are skipped
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
        rowNum = 0;
        rank = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor[] previous = chunk.getPrevious();
        // the row counter always increases
        rowNum++;

        if (isNullOrEmpty(previous)) {
            // if previous result is null, rank increases
            rank++;
        } else if (compareDVDArrays(previous, dvds) != 0) {
            // rank increasing as long as values differ
            // if values are equal, only rowNum is increases
            if (rank != rowNum) {
                rank = rowNum;
            }
        }
        // always collect the now previous value
        chunk.setPrevious(dvds);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor[] dvds) throws StandardException {
        DataValueDescriptor[] result = chunk.getPrevious();
        if (compareDVDArrays(result, dvds) == 0) {
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
        return new RankFunction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(rank);
        out.writeLong(rowNum);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rank = in.readLong();
        rowNum = in.readLong();
    }
}
