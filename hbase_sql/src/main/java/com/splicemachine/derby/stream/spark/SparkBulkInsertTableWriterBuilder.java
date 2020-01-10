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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.output.BulkInsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class SparkBulkInsertTableWriterBuilder<K, V>
        extends InsertTableWriterBuilder implements BulkInsertDataSetWriterBuilder {

    private DataSet dataSet;

    private String bulkImportDirectory;
    private boolean samplingOnly;
    private boolean outputKeysOnly;
    private boolean skipSampling;
    private String indexName;

    public SparkBulkInsertTableWriterBuilder(){
    }

    public SparkBulkInsertTableWriterBuilder(DataSet dataSet){
        this.dataSet = dataSet;
    }
    @Override
    public BulkInsertDataSetWriterBuilder bulkImportDirectory(String bulkImportDirectory) {
        this.bulkImportDirectory = bulkImportDirectory;
        return this;
    }

    @Override
    public BulkInsertDataSetWriterBuilder samplingOnly(boolean samplingOnly) {
        this.samplingOnly = samplingOnly;
        return this;
    }

    @Override
    public BulkInsertDataSetWriterBuilder outputKeysOnly(boolean outputKeysOnly) {
        this.outputKeysOnly = outputKeysOnly;
        return this;
    }

    @Override
    public BulkInsertDataSetWriterBuilder skipSampling(boolean skipSampling) {
        this.skipSampling = skipSampling;
        return this;
    }

    @Override
    public BulkInsertDataSetWriterBuilder indexName(String indexName) {
        this.indexName = indexName;
        return this;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(bulkImportDirectory);
        out.writeBoolean(samplingOnly);
        out.writeBoolean(outputKeysOnly);
        out.writeBoolean(skipSampling);
        out.writeUTF(indexName);
        out.writeDouble(sampleFraction);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        bulkImportDirectory = in.readUTF();
        samplingOnly = in.readBoolean();
        outputKeysOnly = in.readBoolean();
        skipSampling = in.readBoolean();
        indexName = in.readUTF();
        sampleFraction = in.readDouble();
    }

    @Override
    public DataSetWriter build() {
        return new BulkInsertDataSetWriter(dataSet, tableVersion, pkCols, autoIncrementRowLocationArray, heapConglom,
                execRowDefinition, spliceSequences, operationContext, txn, bulkImportDirectory, samplingOnly, outputKeysOnly,
                skipSampling, indexName, sampleFraction, token);
    }
}
