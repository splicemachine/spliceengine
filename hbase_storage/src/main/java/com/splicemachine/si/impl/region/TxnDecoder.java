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
