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

package com.splicemachine.si.api.data;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.si.api.txn.ConflictType;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.DDLFilter;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordScan;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * A Factory for creating transactionally aware operations (e.g. Puts, Scans, Deletes, Gets, etc.)
 *
 * @author Scott Fines
 *         Date: 7/8/14
 */
public interface TxnOperationFactory{
    Txn readTxn(ObjectInput oi) throws IOException;
    void writeTxn(Txn txn, ObjectOutput out) throws IOException;
    ConflictType conflicts(Record potentialRecord, Record existingRecord);
    RecordScan newDataScan();
    Record newRecord(Txn txn, byte[] key);
    Record newRecord(Txn txn, byte[] key, int[] fields, ExecRow data) throws StandardException;
    Record newRecord(Txn txn, byte[] key, ExecRow data) throws StandardException;
    Record newRecord(Txn txn, byte[] key, int[] fields, DataValueDescriptor[] data) throws StandardException;
    Record newRecord(Txn txn, byte[] key, DataValueDescriptor[] data) throws StandardException;
    Record newRecord(Txn txn, byte[] keyObject, byte[] keyOffset, byte[] keyLength, int[] fields, ExecRow data) throws StandardException;
    Record newUpdate(Txn txn, byte[] key);
    Record newDelete(Txn txn, byte[] key);
    DDLFilter newDDLFilter(Txn txn);
    RecordScan readScan(ObjectInput in);
    void writeScan(RecordScan scan, ObjectOutput out);


}
