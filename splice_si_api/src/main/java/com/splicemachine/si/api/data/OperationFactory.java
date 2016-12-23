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

    RecordScan newScan();

    DataGet newGet(byte[] rowKey,DataGet previous);

    DataPut newPut(byte[] rowKey);

    DataPut newPut(ByteSlice slice);

    DataDelete newDelete(byte[] rowKey);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, byte[] value);

    DataCell newCell(byte[] key, byte[] family, byte[] qualifier, long timestamp,byte[] value);

    void writeScan(RecordScan scan, ObjectOutput out) throws IOException;

    RecordScan readScan(ObjectInput in) throws IOException;

    DataResult newResult(List<DataCell> visibleColumns);

    DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp);
}
