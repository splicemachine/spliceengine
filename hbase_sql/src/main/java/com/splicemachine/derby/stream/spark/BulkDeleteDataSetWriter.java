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
package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 5/31/17.
 */
public class BulkDeleteDataSetWriter extends BulkDataSetWriter implements DataSetWriter {
    private String bulkDeleteDirectory;
    private int[] colMap;

    public BulkDeleteDataSetWriter() {}

    public BulkDeleteDataSetWriter(DataSet dataSet, OperationContext operationContext, String bulkDeleteDirectory,
                                   TxnView txn, long heapConglom, int[] colMap) {
        super(dataSet, operationContext, heapConglom, txn, null);
        this.bulkDeleteDirectory = bulkDeleteDirectory;
        this.colMap = colMap;
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException {

        List<Long> allCongloms = Lists.newArrayList();
        ArrayList<DDLMessage.TentativeIndex> tentativeIndexList = new ArrayList();
        getAllConglomerates(allCongloms, tentativeIndexList);

        // get the actual start/end key for each partition after split
        final List<BulkImportPartition> bulkImportPartitions =
                getBulkImportPartitions(allCongloms, bulkDeleteDirectory);
        RowAndIndexGenerator rowAndIndexGenerator = new BulkDeleteRowIndexGenerationFunction(operationContext, txn,
                heapConglom, tentativeIndexList, bulkDeleteDirectory, colMap);

        String compressionAlgorithm = HConfiguration.getConfiguration().getCompressionAlgorithm();
        // Write to HFile
        HFileGenerationFunction hfileGenerationFunction =
                new BulkDeleteHFileGenerationFunction(operationContext, txn.getTxnId(),
                        heapConglom, compressionAlgorithm, bulkImportPartitions);

        DataSet rowAndIndexes = dataSet.flatMap(rowAndIndexGenerator);
        assert rowAndIndexes instanceof SparkDataSet;

        partitionUsingRDDSortUsingDataFrame(bulkImportPartitions, rowAndIndexes, hfileGenerationFunction);
        bulkLoad(bulkImportPartitions, bulkDeleteDirectory, "Delete:");

        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }

    @Override
    public void setTxn(TxnView txn){
        this.txn = txn;
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return operationContext.getTxn();
        else
            return txn;
    }
}
