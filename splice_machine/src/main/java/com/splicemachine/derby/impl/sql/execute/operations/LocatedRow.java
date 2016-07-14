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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.KeyableRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by dgomezferro on 4/14/15.
 */
public class LocatedRow implements KeyableRow, Externalizable {
    private RowLocation rowLocation;
    private ExecRow row;

    public LocatedRow() {}

    public LocatedRow(RowLocation rowLocation, ExecRow row) {
        this.rowLocation = rowLocation;
        this.row = row;
    }

    public LocatedRow(ExecRow row) {
        this(null, row);
    }

    public RowLocation getRowLocation() {
        return rowLocation;
    }

    public ExecRow getRow() {
        return row;
    }

    public void setRow(ExecRow row) {
        this.row = row;
    }

    public void setRowLocation(RowLocation rowLocation) {
        this.rowLocation = rowLocation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LocatedRow locatedRow = (LocatedRow) o;

        if (!row.equals(locatedRow.row)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return row.hashCode();
    }

    @Override
    public ExecRow getKeyedExecRow(int[] ints) throws StandardException {
        return row.getKeyedExecRow(ints);
    }

    @Override
    public String toString() {
        return String.format("LocatedRow {rowLocation=%s, row=%s}",rowLocation,row);
    }

    @Override
    public int hashCode(int[] ints) {
        return row.hashCode(ints);
    }

    @Override
    public int compareTo(int[] ints, ExecRow execRow) {
        return row.compareTo(ints,execRow);
    }

    public LocatedRow getClone() {
        return new LocatedRow(getRowLocation(),getRow());
    }

    public LocatedRow getClone(boolean materialized){
        return materialized?
                new LocatedRow(HBaseRowLocation.deepClone((HBaseRowLocation)getRowLocation()),getRow().getClone()):
                getClone();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(rowLocation);
        out.writeObject(row);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        rowLocation = (RowLocation) in.readObject();
        row = (ExecRow) in.readObject();
    }
}
