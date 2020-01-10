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

import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 1/18/16
 */
public interface OperationFactory{

    DataScan newScan();

    DataGet newGet(byte[] rowKey,DataGet previous);

    DataPut newPut(byte[] rowKey);

    DataPut newPut(ByteSlice slice);

    DataDelete newDelete(byte[] rowKey);

    DataDelete newDelete(ByteSlice rowKey);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, byte[] value);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, long timestamp,byte[] value);

    void writeScan(DataScan scan, ObjectOutput out) throws IOException;

    DataScan readScan(ObjectInput in) throws IOException;

    DataResult newResult(List<DataCell> visibleColumns);

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);
}
