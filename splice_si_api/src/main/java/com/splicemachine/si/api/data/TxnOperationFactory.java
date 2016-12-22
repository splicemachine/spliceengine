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

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.storage.*;
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

    DataScan newDataScan(Txn txn);

    DataGet newDataGet(Txn txn, byte[] rowKey, DataGet previous);

    Txn readTxn(ObjectInput oi) throws IOException;

    void writeTxn(Txn txn, ObjectOutput out) throws IOException;

    void writeScan(DataScan scan, ObjectOutput out) throws IOException;

    DataScan readScan(ObjectInput in) throws IOException;

    DataPut newDataPut(Txn txn, byte[] key) throws IOException;

    DataMutation newDataDelete(Txn txn, byte[] key) throws IOException;

    DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value);
}
