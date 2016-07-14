/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/6/16.
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
