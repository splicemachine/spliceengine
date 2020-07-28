package com.splicemachine.ck.visitor;

import com.splicemachine.ck.Utils;
import com.splicemachine.ck.decoder.UserDataDecoder;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.hadoop.hbase.Cell;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

public class CellPrinter extends ICellVisitor {

    private final UserDataDecoder decoder;
    StringBuilder stringBuilder;
    SortedMap<Long, List<String>> events;

    public CellPrinter(UserDataDecoder decoder) {
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

    @Override
    protected void preVisit(Cell cell) {
        stringBuilder.setLength(0);
    }

    @Override
    protected void postVisit(Cell cell) {
        Long key = cell.getTimestamp();
        events.computeIfAbsent(key, k -> new ArrayList<>());
        events.get(key).add(stringBuilder.toString());
    }

    @Override
    public void visitCommitTimestamp(Cell cell) {
        stringBuilder.append(Utils.Colored.green("commit timestamp "));
        stringBuilder.append(Utils.Colored.green(Long.toString(com.splicemachine.primitives.Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength()))));
    }

    @Override
    public void visitTombstone() {
        stringBuilder.append(Utils.Colored.red("tombstone set"));
    }

    @Override
    public void visitAntiTombstone() {
        stringBuilder.append(Utils.Colored.blue("anti-tombstone set"));
    }

    private String decode(Cell c, UserDataDecoder decoder) throws StandardException {
        ExecRow er = decoder.decode(c);
        return Utils.toString(er);
    }

    @Override
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

    @Override
    public void visitFirstWrite() {
        stringBuilder.append(Utils.Colored.cyan("first write token set"));
    }

    @Override
    public void visitDeleteRightAfterFirstWrite() {
        stringBuilder.append(Utils.Colored.purple("delete right after first right set"));
    }

    @Override
    public void visitForeignKeyCounter() {
        stringBuilder.append("foreign key counter is set");
    }

    @Override
    public void visitOther() {
        stringBuilder.append(Utils.Colored.darkGray("other"));
    }
}
