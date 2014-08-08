package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.loader.ClassFactory;
import org.apache.derby.iapi.sql.execute.ExecAggregator;
import org.apache.derby.iapi.sql.execute.WindowFunction;
import org.apache.derby.iapi.types.DataTypeDescriptor;
import org.apache.derby.iapi.types.DataValueDescriptor;

/**
 * Implementation of RANK -  Ranks each row in a partition. If values in the ranking column are the same,
 * they receive the same rank, however, the next number in the ranking sequence is skipped.
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
    public WindowFunction setup(ClassFactory classFactory, String aggregateName, DataTypeDescriptor returnDataType) {
        super.setup( classFactory, aggregateName, returnDataType );
        return this;
    }

    @Override
    public void accumulate(DataValueDescriptor addend, Object ga) throws StandardException {
        this.add(addend);
    }

    @Override
    public void reset() {
        super.reset();
        rowNum = 0;
        rank = 0;
    }

    @Override
    protected void calculateOnAdd(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (result == null || result.isNull()) {
            // if previous result is null, rank increases
            rank++;
            rowNum++;
        } else if (dvd.compare(result) == 0) {
            // rank increasing as long as values differ
            // if values are equal, only rowNum is increases
            rowNum++;
        } else {
            // values are not equal
            rowNum++;
            if (rank != rowNum) {
                rank = rowNum;
            }
        }
        // always collect the now previous value
        chunk.setResult(dvd);
    }

    @Override
    protected void calculateOnRemove(WindowChunk chunk, DataValueDescriptor dvd) throws StandardException {
        DataValueDescriptor result = chunk.getResult();
        if (dvd.compare(result) == 0) {
            recalculate(chunk);
        }
    }

    private void recalculate(WindowChunk chunk) throws StandardException{
    }

    @Override
    public void merge(ExecAggregator inputAggregator) throws StandardException {

    }

    @Override
    public DataValueDescriptor getResult() throws StandardException {
        // just return the current rank
        WindowChunk first = values.get(0);
        DataValueDescriptor result = first.getResult();
        result = result.cloneValue(false);
        result.setValue(rank);
        return result;
    }

    @Override
    public ExecAggregator newAggregator() {
        return new RankFunction();
    }

    @Override
    public boolean didEliminateNulls() {
        return false;
    }

    @Override
    public WindowFunction newWindowFunction() {
        return new RankFunction();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {

    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {

    }

    @Override
    public int getTypeFormatId() {
        return 0;
    }

}
