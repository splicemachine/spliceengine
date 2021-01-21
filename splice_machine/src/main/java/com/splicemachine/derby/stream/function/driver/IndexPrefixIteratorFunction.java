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

package com.splicemachine.derby.stream.function.driver;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.BaseActivation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.IndexPrefixIteratorOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;

import java.io.*;
import java.util.*;

import static com.splicemachine.EngineDriver.isMemPlatform;
import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

public class IndexPrefixIteratorFunction extends SpliceFlatMapFunction<SpliceOperation,Iterator<ExecRow>, ExecRow> {
    protected IndexPrefixIteratorOperation driverOperation;
    private Iterator<ExecRow> mainIterator;
    private ExecRow currentRow;
    private int firstIndexColumnNumber;
    private int [] baseColumnMap;
    private int firstMappedIndexColumnNumber;

    public IndexPrefixIteratorFunction() {
        super();
    }

    public IndexPrefixIteratorFunction(OperationContext<SpliceOperation> operationContext, int firstIndexColumnNumber) {
        super(operationContext);
        this.firstIndexColumnNumber = firstIndexColumnNumber;
    }

    protected class PrefixAdvancingIterator implements Iterator<ExecRow> {
        private Iterator<ExecRow> sourceIterator;
        private boolean hasNext;

        protected PrefixAdvancingIterator(Iterator<ExecRow> sourceIterator) throws StandardException {
            this.sourceIterator = sourceIterator;
        }

        private void closeSourceIterator() {
            if (sourceIterator instanceof Closeable) {
                Closeable closeable = (Closeable) sourceIterator;
                try {
                    closeable.close();
                }
                catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        @Override
        public boolean hasNext() {
            if (hasNext)
                return true;
            hasNext = sourceIterator.hasNext();
            return hasNext;
        }

        @Override
        public ExecRow next() {
            hasNext = false;
            currentRow = sourceIterator.next();
            ExecRow newRow = new ValueRow(1);
            try {
                newRow.setColumn(1, currentRow.getColumn(firstMappedIndexColumnNumber).cloneValue(true));
            }
            catch (StandardException e) {
                closeSourceIterator();
                throw new RuntimeException(e);
            }
            try {
                // No need to hold on to memory if we are reading multiple
                // index prefixes.  Close the stream right away.
                closeSourceIterator();

                // This code creates a non-inclusive start key for the next probing scan.
                ((BaseActivation) driverOperation.getActivation()).
                 setScanKeyPrefix(newRow.getColumn(1));

                // Start a new probing scan and replace the source iterator with
                // that of the new scan.
                DataSet<ExecRow> ds = driverOperation.getDriverDataSet(driverOperation.createTableScannerBuilder());
                sourceIterator = ds.toLocalIterator();
            }
            catch (Exception e) {
                closeSourceIterator();
                throw new RuntimeException(StandardException.newException(LANG_INTERNAL_ERROR, e,
                           "Error setting scanKeyPrefix in IndexPrefixIteratorFunction."));
            }
            return newRow;
        }
    }

    @Override
    public Iterator<ExecRow> call(Iterator<ExecRow> locatedRows) throws Exception {
        if (mainIterator == null) {
            mainIterator = new PrefixAdvancingIterator(locatedRows);
            driverOperation = (IndexPrefixIteratorOperation) getOperation();
            if (!mainIterator.hasNext())
                return Collections.EMPTY_LIST.iterator();

            baseColumnMap = driverOperation.getScanSetBuilder().getBaseColumnMap();
            firstMappedIndexColumnNumber = baseColumnMap[firstIndexColumnNumber-1]+1;
            if (firstMappedIndexColumnNumber < 1)
                throw StandardException.newException(LANG_INTERNAL_ERROR,
                           "firstMappedIndexColumnNumber less than 1 in IndexPrefixIteratorFunction.");
        }
        return mainIterator;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(firstIndexColumnNumber);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        firstIndexColumnNumber = in.readInt();
    }
}
