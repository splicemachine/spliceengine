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

package com.splicemachine.si.impl.region;

import com.splicemachine.si.coprocessor.TxnMessage;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;

import java.io.IOException;
import java.util.List;

/**
 * Interface for different mechanisms for encoding/decoding
 * transactions from the transaction table storage.
 *
 * This is an interface so that we can support both the
 * old (non-packed) table format and the new format
 * simultaneously.
 *
 * @author Scott Fines
 * Date: 8/14/14
 *
 */

public interface TxnDecoder {

    TxnMessage.Txn decode(RegionTxnStore txnStore,long txnId, Result result) throws IOException;

    TxnMessage.Txn decode(RegionTxnStore txnStore,List<Cell> keyValues) throws IOException;

	Put encodeForPut(TxnMessage.TxnInfo txn,byte[] rowKey) throws IOException;

}
