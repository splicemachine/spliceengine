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

package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.HBaseBulkImporter;
import com.splicemachine.derby.stream.output.HBaseBulkImporterBuilder;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.spark.api.java.JavaRDD;

public class SparkHBaseBulkImporterBuilder<V> implements HBaseBulkImporterBuilder{

    private DataSet<V> dataSet;

    private String tableVersion;
    private int[] pkCols;
    private RowLocation[] autoIncrementRowLocationArray;
    private long heapConglom;
    private ExecRow execRow;
    private SpliceSequence[] spliceSequences;
    private OperationContext operationContext;
    private TxnView txn;
    private String bulkImportDirectory;
    private boolean samplingOnly;

    public SparkHBaseBulkImporterBuilder(){
    }

    public SparkHBaseBulkImporterBuilder(DataSet<V> dataSet){
        this.dataSet = dataSet;
    }
    @Override
    public HBaseBulkImporterBuilder tableVersion(String tableVersion) {
        this.tableVersion = tableVersion;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder pkCols(int[] pkCols) {
        this.pkCols = pkCols;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder autoIncrementRowLocationArray(RowLocation[] autoIncrementRowLocationArray) {
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder heapConglom(long heapConglom) {
        this.heapConglom = heapConglom;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder execRow(ExecRow execRow) {
        this.execRow = execRow;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder sequences(SpliceSequence[] spliceSequences) {
        this.spliceSequences = spliceSequences;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder operationContext(OperationContext operationContext) {
        this.operationContext = operationContext;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder txn(TxnView txn) {
        this.txn = txn;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder bulkImportDirectory(String bulkImportDirectory) {
        this.bulkImportDirectory = bulkImportDirectory;
        return this;
    }

    @Override
    public HBaseBulkImporterBuilder samplingOnly(boolean samplingOnly) {
        this.samplingOnly = samplingOnly;
        return this;
    }

    @Override
    public HBaseBulkImporter build() {
        return new SparkHBaseBulkImport(dataSet, tableVersion, pkCols, autoIncrementRowLocationArray, heapConglom,
                execRow, spliceSequences, operationContext, txn, bulkImportDirectory, samplingOnly);
    }
}
