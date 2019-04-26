/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.KeyValue;

import java.util.List;

/**
 * Created by jyuan on 6/2/17.
 */
public class BulkDeleteHFileGenerationFunction extends HFileGenerationFunction {

    public BulkDeleteHFileGenerationFunction() {}

    public BulkDeleteHFileGenerationFunction(OperationContext operationContext,
                                             long txnId,
                                             Long heapConglom,
                                             String compressionAlgorithm,
                                             List<BulkImportPartition> partitionList) {
        super(operationContext, txnId, heapConglom, compressionAlgorithm, partitionList);
        this.operationType = OperationType.DELETE;
    }

    @Override
    protected void writeToHFile (byte[] rowKey, byte[] value) throws Exception {
        KeyValue kv = new KeyValue(rowKey, SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, txnId, value);
        writer.append(kv);
    }
}
