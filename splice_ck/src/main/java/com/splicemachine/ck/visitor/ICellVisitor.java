package com.splicemachine.ck.visitor;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.storage.CellType;
import org.apache.hadoop.hbase.Cell;

public abstract class ICellVisitor {

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

    protected void preVisit(Cell cell) { }

    protected void postVisit(Cell cell) { }

    protected void visitCommitTimestamp(Cell cell) { }

    protected void visitTombstone() { }

    protected void visitAntiTombstone() { }

    protected void visitUserData(Cell userData) throws StandardException { }

    protected void visitFirstWrite() { }

    protected void visitDeleteRightAfterFirstWrite() { }

    protected void visitForeignKeyCounter() { }

    protected void visitOther() { }
}
