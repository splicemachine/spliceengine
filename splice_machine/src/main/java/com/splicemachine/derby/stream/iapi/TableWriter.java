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

package com.splicemachine.derby.stream.iapi;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.si.api.txn.Txn;
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
    void setTxn(Txn txn);
    Txn getTxn();
    byte[] getDestinationTable();
    OperationContext getOperationContext();
}
