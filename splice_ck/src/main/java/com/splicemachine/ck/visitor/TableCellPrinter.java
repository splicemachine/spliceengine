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

package com.splicemachine.ck.visitor;

import com.splicemachine.ck.Utils;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.CellType;
import com.splicemachine.utils.Pair;
import org.apache.commons.codec.binary.Hex;
import org.apache.hadoop.hbase.Cell;

import java.util.*;

class TableCellPrinter {

    public void visit(Cell cell) throws StandardException {
        preVisit(cell);

        CellType cellType = CellUtils.getKeyValueType(cell);
        switch (cellType) {
            case COMMIT_TIMESTAMP:
                visitCommitTimestamp(cell);
                break;
            case TOMBSTONE:
                visitTombstone();
                break;
            case ANTI_TOMBSTONE:
                visitAntiTombstone();
                break;
            case USER_DATA:
                visitUserData(cell);
                break;
            case FIRST_WRITE_TOKEN:
                visitFirstWrite();
                break;
            case DELETE_RIGHT_AFTER_FIRST_WRITE_TOKEN:
                visitDeleteRightAfterFirstWrite();
                break;
            case FOREIGN_KEY_COUNTER:
                visitForeignKeyCounter();
                break;
            case OTHER:
                visitOther();
                break;
        }

        postVisit(cell);
    }

    private final UserDataDecoder decoder;
    StringBuilder stringBuilder;
    SortedMap<Long, List<String>> events;
    boolean hbase;

    public TableCellPrinter(UserDataDecoder decoder, boolean hbase) {
        this.decoder = decoder;
        this.stringBuilder = new StringBuilder();
        events = new TreeMap<>();
        this.hbase = hbase;
    }

    public String getOutput() {
        StringBuilder sb = new StringBuilder();
        for(Map.Entry<Long, List<String>> entry : events.entrySet()) {
            sb.append(Utils.Colored.boldWhite("at: ")).append(Utils.Colored.boldWhite(Long.toString(entry.getKey()))).append("\n");
            for(String eventResult : entry.getValue()) {
                sb.append("\t").append(eventResult).append("\n");
            }
        }
        return sb.toString();
    }

    protected void preVisit(Cell cell) {
        stringBuilder.setLength(0);
    }
    public static byte[] getSliceOfArray(byte[] arr,
                                        int start, int len)
    {

        // Get the slice of the Array
        byte[] slice = new byte[len];

        // Copy elements of arr to slice
        for (int i = 0; i < slice.length; i++) {
            slice[i] = arr[start + i];
        }

        // return the slice
        return slice;
    }

    protected void postVisit(Cell cell) {
        Long key = cell.getTimestamp();
        events.computeIfAbsent(key, k -> new ArrayList<>());
        byte[] b = cell.getRowArray();
        events.get(key).add(stringBuilder.toString());
        if(hbase) {
            events.get(key).add("  column=" + com.splicemachine.primitives.Bytes.toStringBinary(getSliceOfArray(b, cell.getFamilyOffset(), cell.getFamilyLength()))
                    + ":" + com.splicemachine.primitives.Bytes.toStringBinary(getSliceOfArray(b, cell.getQualifierOffset(), cell.getQualifierLength()))
                    + ", value = " + com.splicemachine.primitives.Bytes.toStringBinary(getSliceOfArray(b, cell.getValueOffset(), cell.getValueLength())));
        }

    }

    public void visitCommitTimestamp(Cell cell) {

        stringBuilder.append(Utils.Colored.green("commit timestamp "));
        stringBuilder.append(Utils.Colored.green(Long.toString(CellUtils.getCommitTimestamp(cell))));
    }

    public void visitTombstone() {
        stringBuilder.append(Utils.Colored.red("tombstone set"));
    }

    public void visitAntiTombstone() {
        stringBuilder.append(Utils.Colored.blue("anti-tombstone set"));
    }

    private String decode(Cell c, UserDataDecoder decoder) throws StandardException {
        Pair<BitSet, ExecRow> pair = decoder.decode(c);
        return Utils.toString(pair);
    }

    public void visitUserData(Cell userData) throws StandardException {
        stringBuilder.append(Utils.Colored.yellow("user data "));
        if(decoder != null ) {
            stringBuilder.append(Utils.Colored.yellow("parsed "));
            stringBuilder.append(decode(userData, decoder));
        } else {
            stringBuilder.append(Utils.Colored.yellow("in hex "));
            stringBuilder.append( CellUtils.getUserDataHex(userData) );
        }
    }

    public void visitFirstWrite() {
        stringBuilder.append(Utils.Colored.cyan("first write token set"));
    }

    public void visitDeleteRightAfterFirstWrite() {
        stringBuilder.append(Utils.Colored.purple("delete right after first write set"));
    }

    public void visitForeignKeyCounter() {
        stringBuilder.append("foreign key counter set");
    }

    public void visitOther() {
        stringBuilder.append(Utils.Colored.darkGray("other"));
    }
}
