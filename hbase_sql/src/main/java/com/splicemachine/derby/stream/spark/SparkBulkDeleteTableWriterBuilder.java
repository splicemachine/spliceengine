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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.output.BulkDeleteDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;

/**
 * Created by jyuan on 5/31/17.
 */
public class SparkBulkDeleteTableWriterBuilder extends DeleteTableWriterBuilder implements BulkDeleteDataSetWriterBuilder{

    private DataSet dataSet;
    private String bulkDeleteDirectory;
    private int[] colMap;

    public SparkBulkDeleteTableWriterBuilder() {}

    public SparkBulkDeleteTableWriterBuilder(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    @Override
    public BulkDeleteDataSetWriterBuilder bulkDeleteDirectory(String bulkDeleteDirectory) {
        this.bulkDeleteDirectory = bulkDeleteDirectory;
        return this;
    }

    @Override
    public BulkDeleteDataSetWriterBuilder colMap(int[] colMap) {
        this.colMap = colMap;
        return this;
    }

    public DataSetWriter build() throws StandardException {
        return new BulkDeleteDataSetWriter(dataSet, operationContext, bulkDeleteDirectory, txn, heapConglom, colMap);
    }
}
