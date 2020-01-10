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

package com.splicemachine.si.api.data;

import com.splicemachine.si.api.txn.TxnView;
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

    DataScan newDataScan(TxnView txn);

    DataGet newDataGet(TxnView txn,byte[] rowKey,DataGet previous);

    TxnView fromReads(Attributable op) throws IOException;

    TxnView fromWrites(Attributable op) throws IOException;

    TxnView fromWrites(byte[] data,int off,int length) throws IOException;

    TxnView fromReads(byte[] data,int off,int length) throws IOException;

    TxnView readTxn(ObjectInput oi) throws IOException;

    TxnView readTxnStack(ObjectInput oi) throws IOException;

    void writeTxn(TxnView txn,ObjectOutput out) throws IOException;

    void writeTxnStack(TxnView txn,ObjectOutput out) throws IOException;

    void writeScan(DataScan scan, ObjectOutput out) throws IOException;

    DataScan readScan(ObjectInput in) throws IOException;

    byte[] encode(TxnView txn);

    void encodeForReads(Attributable attributable,TxnView txn, boolean isCountStar);

    void encodeForWrites(Attributable attributable,TxnView txn) throws IOException;

    TxnView decode(byte[] data,int offset,int length);

    DataPut newDataPut(TxnView txn,byte[] key) throws IOException;

    DataMutation newDataDelete(TxnView txn,byte[] key) throws IOException;

    DataCell newDataCell(byte[] key,byte[] family,byte[] qualifier,byte[] value);
}
