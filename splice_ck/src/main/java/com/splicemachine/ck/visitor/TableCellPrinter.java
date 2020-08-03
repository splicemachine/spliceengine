package com.splicemachine.ck.visitor;

import com.splicemachine.ck.Utils;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.CellType;
import com.splicemachine.utils.Pair;
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

    public TableCellPrinter(UserDataDecoder decoder) {
        this.decoder = decoder;
        this.stringBuilder = new StringBuilder();
        events = new TreeMap<>();
    }

    public String getOutput() {
        StringBuilder sb = new StringBuilder();
        for (long event : events.keySet()) {
            sb.append(Utils.Colored.boldWhite("at: ")).append(Utils.Colored.boldWhite(Long.toString(event))).append("\n");
            for(String eventResult : events.get(event)) {
                sb.append("\t").append(eventResult).append("\n");
            }
        }
        return sb.toString();
    }

    protected void preVisit(Cell cell) {
        stringBuilder.setLength(0);
    }

    protected void postVisit(Cell cell) {
        Long key = cell.getTimestamp();
        events.computeIfAbsent(key, k -> new ArrayList<>());
        events.get(key).add(stringBuilder.toString());
    }

    public void visitCommitTimestamp(Cell cell) {
        stringBuilder.append(Utils.Colored.green("commit timestamp "));
        stringBuilder.append(Utils.Colored.green(Long.toString(com.splicemachine.primitives.Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()))));
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
            stringBuilder.append(com.splicemachine.primitives.Bytes.toStringBinary(userData.getValueArray()));
        }
    }

    public void visitFirstWrite() {
        stringBuilder.append(Utils.Colored.cyan("first write token set"));
    }

    public void visitDeleteRightAfterFirstWrite() {
        stringBuilder.append(Utils.Colored.purple("delete right after first right set"));
    }

    public void visitForeignKeyCounter() {
        stringBuilder.append("foreign key counter set");
    }

    public void visitOther() {
        stringBuilder.append(Utils.Colored.darkGray("other"));
    }
}
