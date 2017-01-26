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

package com.splicemachine.derby.stream.output;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;

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
