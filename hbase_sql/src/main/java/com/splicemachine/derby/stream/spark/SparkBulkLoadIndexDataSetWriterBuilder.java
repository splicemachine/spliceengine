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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.BulkLoadIndexDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.si.api.txn.TxnView;

/**
 * Created by jyuan on 10/6/17.
 */
public class SparkBulkLoadIndexDataSetWriterBuilder implements BulkLoadIndexDataSetWriterBuilder {

    private DataSet dataSet;
    private String  bulkLoadDirectory;
    private boolean sampling;
    private long destConglomerate;
    private TxnView txn;
    private OperationContext operationContext;
    private DDLMessage.TentativeIndex tentativeIndex;
    private String indexName;
    private String tableVersion;

    public SparkBulkLoadIndexDataSetWriterBuilder(DataSet dataSet){
        this.dataSet = dataSet;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder bulkLoadDirectory(String bulkLoadDirectory){
        this.bulkLoadDirectory = bulkLoadDirectory;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder sampling(boolean sampling) {
        this.sampling = sampling;
        return this;
    }


    @Override
    public BulkLoadIndexDataSetWriterBuilder destConglomerate(long destConglomerate) {
        this.destConglomerate = destConglomerate;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder txn(TxnView txn){
        this.txn = txn;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder tentativeIndex(DDLMessage.TentativeIndex tentativeIndex) {
        this.tentativeIndex = tentativeIndex;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    @Override
    public BulkLoadIndexDataSetWriterBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    @Override
    public DataSetWriter build() throws StandardException {
        return new BulkLoadIndexDataSetWriter(dataSet, bulkLoadDirectory, sampling,
                destConglomerate, txn, operationContext, tentativeIndex, indexName, tableVersion);
    }
}
