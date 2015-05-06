package com.splicemachine.derby.stream.temporary;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriter;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;

import java.util.Iterator;

/**
 * Created by jleach on 5/6/15.
 */
public class SpliceWriteApi {

    public static ExecRow getEmptyRow(String schemaName, String tableName) {
        return null;
    }
    public static String[] getColumnNames(String schemaName, String tableName) {
        return null;
    }
    public static TxnView getTransaction(Txn.IsolationLevel isolationLevel) {
        return null;
    }

    public static TxnView rollbackTransaction(TxnView txn) {
        return null;
    }

    public static TxnView commitTransaction(TxnView txn) {
        return null;
    }

    public void insert(ExecRow row, TxnView txn) {

    }

    public void update(ExecRow row, TxnView txn) {

    }

    public void delete(RowLocation rowLocation, TxnView txn) {

    }

    public void insert(Iterator<ExecRow> row, TxnView txn) {

    }

    public void update(Iterator<ExecRow> row, TxnView txn) {

    }

    public void delete(Iterator<RowLocation> rowLocation, TxnView txn) {

    }



}
