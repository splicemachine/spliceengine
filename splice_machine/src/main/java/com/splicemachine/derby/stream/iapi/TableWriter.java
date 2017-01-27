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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.si.api.txn.TxnView;
import java.util.Iterator;

/**
 * Created by jleach on 5/20/15.
 */
public interface TableWriter <T> {
    enum Type {INSERT,UPDATE,DELETE,INDEX}

    void open() throws StandardException;
    void open(TriggerHandler triggerHandler,SpliceOperation dmlWriteOperation) throws StandardException;
    void close() throws StandardException;
    void write(T row) throws StandardException;
    void write(Iterator<T> rows) throws StandardException;
    void setTxn(TxnView txn);
    TxnView getTxn();
    byte[] getDestinationTable();
    OperationContext getOperationContext();
}
