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

package org.apache.spark.sql.execution.vectorized;

import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class ColumnarBatch {

    private int numRows;
    private final WritableColumnVector[] columns;

    public static ColumnarBatch allocate(StructType schema, MemoryMode memMode, int maxRows) {
        return new ColumnarBatch(schema, maxRows, memMode);
    }

    /**
     * Returns an iterator over the rows in this batch.
     */
    public Iterator<InternalRow> rowIterator() {
        final int maxRows = numRows;
        final MutableColumnarRow row = new MutableColumnarRow(columns);
        return new Iterator<InternalRow>() {
            int rowId = 0;

            @Override
            public boolean hasNext() {
                return rowId < maxRows;
            }

            @Override
            public InternalRow next() {
                if (rowId >= maxRows) {
                    throw new NoSuchElementException();
                }
                row.rowId = rowId++;
                return row;
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Sets the number of rows in this batch.
     */
    public void setNumRows(int numRows) {
        this.numRows = numRows;
    }

    /**
     * Sets (replaces) the column at `ordinal` with column. This can be used to do very efficient
     * projections.
     */
    public void setColumn(int ordinal, ColumnVector column) {
        columns[ordinal] = column.getDelegate();
    }

    private ColumnarBatch(StructType schema, int maxRows, MemoryMode memMode) {
        this.numRows = maxRows;
        this.columns = new WritableColumnVector[schema.size()];
    }
}
