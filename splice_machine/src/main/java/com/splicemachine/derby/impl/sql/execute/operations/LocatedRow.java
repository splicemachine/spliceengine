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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.KeyableRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import scala.collection.Map;
import scala.collection.Seq;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

/**
 * Created by dgomezferro on 4/14/15.
 */
public class LocatedRow implements KeyableRow, Externalizable, Row {
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

    @Override
    public int size() {
        return row.size();
    }

    @Override
    public int length() {
        return row.length();
    }

    @Override
    public StructType schema() {
        return row.schema();
    }

    @Override
    public Object apply(int i) {
        return row.apply(i);
    }

    @Override
    public Object get(int i) {
        return row.get(i);
    }

    @Override
    public boolean isNullAt(int i) {
        return row.isNullAt(i);
    }

    @Override
    public boolean getBoolean(int i) {
        return row.getBoolean(i);
    }

    @Override
    public byte getByte(int i) {
        return row.getByte(i);
    }

    @Override
    public short getShort(int i) {
        return row.getShort(i);
    }

    @Override
    public int getInt(int i) {
        return row.getInt(i);
    }

    @Override
    public long getLong(int i) {
        return row.getLong(i);
    }

    @Override
    public float getFloat(int i) {
        return row.getFloat(i);
    }

    @Override
    public double getDouble(int i) {
        return row.getDouble(i);
    }

    @Override
    public String getString(int i) {
        return row.getString(i);
    }

    @Override
    public BigDecimal getDecimal(int i) {
        return row.getDecimal(i);
    }

    @Override
    public Date getDate(int i) {
        return row.getDate(i);
    }

    @Override
    public Timestamp getTimestamp(int i) {
        return row.getTimestamp(i);
    }

    @Override
    public <T> Seq<T> getSeq(int i) {
        return row.getSeq(i);
    }

    @Override
    public <T> List<T> getList(int i) {
        return row.getList(i);
    }

    @Override
    public <K, V> Map<K, V> getMap(int i) {
        return row.getMap(i);
    }

    @Override
    public <K, V> java.util.Map<K, V> getJavaMap(int i) {
        return row.getJavaMap(i);
    }

    @Override
    public Row getStruct(int i) {
        return row.getStruct(i);
    }

    @Override
    public <T> T getAs(int i) {
        return row.getAs(i);
    }

    @Override
    public <T> T getAs(String s) {
        return row.getAs(s);
    }

    @Override
    public int fieldIndex(String s) {
        return row.fieldIndex(s);
    }

    @Override
    public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> seq) {
        return row.getValuesMap(seq);
    }

    @Override
    public Row copy() {
        return this.getClone();
    }

    @Override
    public boolean anyNull() {
        return row.anyNull();
    }

    @Override
    public Seq<Object> toSeq() {
        return row.toSeq();
    }

    @Override
    public String mkString() {
        return row.mkString();
    }

    @Override
    public String mkString(String s) {
        return row.mkString(s);
    }

    @Override
    public String mkString(String s, String s1, String s2) {
        return row.mkString(s,s1,s2);
    }
}
