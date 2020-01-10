
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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.ActivationHolder;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.utils.BulkLoadUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import scala.Tuple2;

import java.util.Collections;
import java.util.List;

/**
 * Created by jyuan on 10/6/17.
 */
public class BulkLoadIndexDataSetWriter extends BulkDataSetWriter implements DataSetWriter {
    private String  bulkLoadDirectory;
    private boolean sampling;
    private DDLMessage.TentativeIndex tentativeIndex;
    private String indexName;
    private String tableVersion;

    public BulkLoadIndexDataSetWriter(DataSet dataSet,
                                      String  bulkLoadDirectory,
                                      boolean sampling,
                                      long destConglomerate,
                                      TxnView txn,
                                      OperationContext operationContext,
                                      DDLMessage.TentativeIndex tentativeIndex,
                                      String indexName,
                                      String tableVersion) {

        super(dataSet, operationContext, destConglomerate, txn, null);
        this.bulkLoadDirectory = bulkLoadDirectory;
        this.sampling = sampling;
        this.tentativeIndex = tentativeIndex;
        this.indexName = indexName;
        this.tableVersion = tableVersion;
    }

    @Override
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }

    @Override
    public void setTxn(TxnView childTxn){
        this.txn = childTxn;
    }

    @Override
    public TxnView getTxn(){
        if(txn==null)
            return operationContext.getTxn();
        else
            return txn;
    }

    @Override
    public DataSet<ExecRow> write() throws StandardException {

        List<Long> allCongloms = Lists.newArrayList();
        allCongloms.add(heapConglom);

        if (sampling) {
            sampleAndSplitIndex();
        }
        final List<BulkImportPartition> bulkLoadPartitions =
                getBulkImportPartitions(allCongloms, bulkLoadDirectory);
        String compressionAlgorithm = HConfiguration.getConfiguration().getCompressionAlgorithm();

        // Write to HFile
        HFileGenerationFunction hfileGenerationFunction =
                new BulkLoadIndexHFileGenerationFunction(operationContext, txn.getTxnId(),
                        heapConglom, compressionAlgorithm, bulkLoadPartitions, tableVersion, tentativeIndex);

        DataSet rowAndIndexes = dataSet
                .map(new IndexTransformFunction(tentativeIndex), null, false, true,
                        String.format("Create Index %s: Generate HFiles", indexName))
                .mapPartitions(new BulkLoadKVPairFunction(heapConglom), false, true,
                        String.format("Create Index %s: Generate HFiles", indexName));

        partitionUsingRDDSortUsingDataFrame(bulkLoadPartitions, rowAndIndexes, hfileGenerationFunction);
        bulkLoad(bulkLoadPartitions, bulkLoadDirectory, String.format("Create Index %s:", indexName));

        ValueRow valueRow=new ValueRow(1);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    private void sampleAndSplitIndex() throws StandardException {
        Activation activation = operationContext.getActivation();
        LanguageConnectionContext lcc = activation.getLanguageConnectionContext();
        double sampleFraction = BulkLoadUtils.getSampleFraction(lcc);
        DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);
        DataSet sampleRowAndIndexes = sampledDataSet
                .map(new IndexTransformFunction(tentativeIndex), null, false, true,
                        String.format("Create Index %s: Sample Data", indexName))
                .mapPartitions(new BulkLoadKVPairFunction(heapConglom), false, true,
                        String.format("Create Index %s: Sample Data", indexName));

        // collect statistics for encoded key/value, include size and histgram
        RowKeyStatisticsFunction statisticsFunction =
                new RowKeyStatisticsFunction(heapConglom, Lists.newArrayList());
        DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(statisticsFunction);

        List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> result = keyStatistics.collect();

        // Calculate cut points for main table and index tables
        List<Tuple2<Long, byte[][]>> cutPoints = BulkLoadUtils.getCutPoints(sampleFraction, result);

        // dump cut points to file system for reference
        ImportUtils.dumpCutPoints(cutPoints, bulkLoadDirectory);

        if (cutPoints != null && cutPoints.size() > 0) {
            BulkLoadUtils.splitTables(cutPoints);
        }
    }
}
