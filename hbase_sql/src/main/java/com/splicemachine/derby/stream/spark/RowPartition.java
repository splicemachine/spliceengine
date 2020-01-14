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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 *
 * Row Partition Implementation.
 *
 */
    public class RowPartition implements Comparable<ExecRow>, Externalizable {
        ExecRow firstRow;
        ExecRow lastRow;

        public RowPartition(ExecRow firstRow, ExecRow lastRow) {
            this.firstRow = firstRow;
            this.lastRow = lastRow;
        }

        public RowPartition() {
        }

        @Override
        public int compareTo(ExecRow o) {
            int comparison;
            if (lastRow != null) {
                comparison = compareTo(lastRow,o);
                if (comparison <= 0) return -1;
            }
            if (firstRow != null) {
                comparison = compareTo(firstRow,o);
                if (comparison >= 0) return comparison;
            }
            return 0;
        }


        public int compareTo(ExecRow partitionRow, ExecRow row) {
            if (row == null)
                return -1;
            int compare;
            assert partitionRow.nColumns() == row.nColumns():"Row Mismatch";
            for (int i = 0; i < partitionRow.nColumns(); i++ ) {
                try {
                    compare = partitionRow.getColumn(i+1).compare(row.getColumn(i+1));
                    if (compare != 0)
                        return compare;
                } catch (StandardException e) {
                    throw new RuntimeException(e);
                }
            }
            return 0;
        }

        @Override
        public void writeExternal(ObjectOutput out) throws IOException {
            try {
                out.writeBoolean(firstRow != null);
                if (firstRow != null)
                    out.writeObject(firstRow);
                out.writeBoolean(lastRow != null);
                if (lastRow != null)
                    out.writeObject(lastRow);
            } catch (Exception e) {
                throw new IOException(e);
            }
        }

        @Override
        public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
            try {
                if (in.readBoolean())
                    firstRow = (ExecRow) in.readObject();
                if (in.readBoolean())
                    lastRow = (ExecRow) in.readObject();
            } catch (Exception e) {
                throw new IOException(e);
            }
        }
    }
