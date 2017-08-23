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

import com.clearspring.analytics.util.Lists;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.ConglomerateDescriptorList;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.stats.ColumnStatisticsImpl;
import com.splicemachine.db.iapi.stats.ColumnStatisticsMerge;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.SQLBlob;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import com.yahoo.sketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.*;
import java.net.URI;
import java.util.*;

public class BulkInsertDataSetWriter extends BulkDataSetWriter implements DataSetWriter {

    private static final Logger LOG=Logger.getLogger(BulkInsertDataSetWriter.class);

    private String tableVersion;
    private int[] pkCols;
    private RowLocation[] autoIncrementRowLocationArray;
    private ExecRow execRow;
    private SpliceSequence[] spliceSequences;
    private String bulkImportDirectory;
    private boolean samplingOnly;
    private boolean outputKeysOnly;
    private boolean skipSampling;
    private String indexName;

    public BulkInsertDataSetWriter(){
    }

    public BulkInsertDataSetWriter(DataSet dataSet,
                                   String tableVersion,
                                   int[] pkCols,
                                   RowLocation[] autoIncrementRowLocationArray,
                                   long heapConglom,
                                   ExecRow execRow,
                                   SpliceSequence[] spliceSequences,
                                   OperationContext operationContext,
                                   TxnView txn,
                                   String bulkImportDirectory,
                                   boolean samplingOnly,
                                   boolean outputKeysOnly,
                                   boolean skipSampling,
                                   String indexName) {
        super(dataSet, operationContext, heapConglom, txn);
        this.tableVersion = tableVersion;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.pkCols = pkCols;
        this.execRow = execRow;
        this.spliceSequences = spliceSequences;
        this.bulkImportDirectory = bulkImportDirectory;
        this.samplingOnly = samplingOnly;
        this.outputKeysOnly = outputKeysOnly;
        this.skipSampling = skipSampling;
        this.indexName = indexName;
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
    public byte[] getDestinationTable(){
        return Bytes.toBytes(heapConglom);
    }

    /**
     *  1) Sample data to calculate key/value size and histogram for keys
     *  2) Calculate cut points and split table and indexes
     *  3) Read and encode data for table and indexes, hash to the partition where its rowkey falls into
     *  4) Sort keys in each partition and write to HFiles
     *  5) Load HFiles to HBase
     * @return
     * @throws StandardException
     */
    @Override
    public DataSet<ExecRow> write() throws StandardException {

        // collect index information for the table
        Activation activation = operationContext.getActivation();
        DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
        ConglomerateDescriptor cd = dd.getConglomerateDescriptor(heapConglom);
        TableDescriptor td = dd.getTableDescriptor(cd.getTableID());
        ConglomerateDescriptorList list = td.getConglomerateDescriptorList();
        List<Long> allCongloms = Lists.newArrayList();
        allCongloms.add(td.getHeapConglomerateId());
        ArrayList<DDLMessage.TentativeIndex> tentativeIndexList = new ArrayList();
        
        if (outputKeysOnly) {
            // This option is for calculating split row keys in HBase format, provided that split row keys are
            // stored in a csv file
            long indexConglom = -1;
            boolean isUnique = false;
            for (ConglomerateDescriptor searchCD :list) {
                if (searchCD.isIndex() && !searchCD.isPrimaryKey() && indexName != null &&
                        searchCD.getObjectName().compareToIgnoreCase(indexName) == 0) {
                    indexConglom = searchCD.getConglomerateNumber();
                    DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                            activation.getLanguageConnectionContext(),
                            td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                            td, searchCD.getIndexDescriptor());
                    isUnique = searchCD.getIndexDescriptor().isUnique();
                    tentativeIndexList.add(ddlChange.getTentativeIndex());
                    allCongloms.add(searchCD.getConglomerateNumber());
                    break;
                }
            }
            RowAndIndexGenerator rowAndIndexGenerator =
                    new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow, autoIncrementRowLocationArray,
                            spliceSequences, heapConglom, txn, operationContext, tentativeIndexList);
            DataSet rowAndIndexes = dataSet.flatMap(rowAndIndexGenerator);
            DataSet keys = rowAndIndexes.mapPartitions(
                    new RowKeyGenerator(bulkImportDirectory, heapConglom, indexConglom, isUnique));
            List<String> files = keys.collect();
        }
        else {
            for (ConglomerateDescriptor searchCD :list) {
                if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                    DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                            activation.getLanguageConnectionContext(),
                            td.getHeapConglomerateId(), searchCD.getConglomerateNumber(),
                            td, searchCD.getIndexDescriptor());
                    tentativeIndexList.add(ddlChange.getTentativeIndex());
                    allCongloms.add(searchCD.getConglomerateNumber());
                }
            }
            List<Tuple2<Long, byte[][]>> cutPoints = null;
            if (!skipSampling) {
                SConfiguration sConfiguration = HConfiguration.getConfiguration();
                double sampleFraction = sConfiguration.getBulkImportSampleFraction();
                DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);

                // encode key/vale pairs for table and indexes
                RowAndIndexGenerator rowAndIndexGenerator =
                        new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow, autoIncrementRowLocationArray,
                                spliceSequences, heapConglom, txn, operationContext, tentativeIndexList);
                DataSet sampleRowAndIndexes = sampledDataSet.flatMap(rowAndIndexGenerator);

                // collect statistics for encoded key/value, include size and histgram
                RowKeyStatisticsFunction statisticsFunction =
                        new RowKeyStatisticsFunction(td.getHeapConglomerateId(), tentativeIndexList);
                DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(statisticsFunction);

                List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> result = keyStatistics.collect();

                // Calculate cut points for main table and index tables
                cutPoints = getCutPoints(sampleFraction, result);

                // dump cut points to file system for reference
                dumpCutPoints(cutPoints);
            }
            if (!samplingOnly && !outputKeysOnly) {

                // split table and indexes using the calculated cutpoints
                if (cutPoints != null && !cutPoints.isEmpty()) {
                    splitTables(cutPoints);
                }

                // get the actual start/end key for each partition after split
                final List<BulkImportPartition> bulkImportPartitions =
                        getBulkImportPartitions(allCongloms, bulkImportDirectory);

                RowAndIndexGenerator rowAndIndexGenerator = new BulkInsertRowIndexGenerationFunction(pkCols, tableVersion, execRow,
                        autoIncrementRowLocationArray, spliceSequences, heapConglom, txn,
                        operationContext, tentativeIndexList);

                String compressionAlgorithm = HConfiguration.getConfiguration().getCompressionAlgorithm();

                // Write to HFile
                HFileGenerationFunction hfileGenerationFunction =
                        new BulkInsertHFileGenerationFunction(operationContext, txn.getTxnId(),
                                heapConglom, compressionAlgorithm, bulkImportPartitions);

                partitionUsingRDDSortUsingDataFrame(bulkImportPartitions, rowAndIndexGenerator, hfileGenerationFunction);

                bulkLoad(bulkImportPartitions, bulkImportDirectory);
            }
        }
        ValueRow valueRow = new ValueRow(3);
        valueRow.setColumn(1, new SQLLongint(operationContext.getRecordsWritten()));
        valueRow.setColumn(2, new SQLLongint());
        valueRow.setColumn(3, new SQLVarchar());
        InsertOperation insertOperation = ((InsertOperation) operationContext.getOperation());
        if (insertOperation != null && operationContext.isPermissive()) {
            long numBadRecords = operationContext.getBadRecords();
            valueRow.setColumn(2, new SQLLongint(numBadRecords));
            if (numBadRecords > 0) {
                String fileName = operationContext.getStatusDirectory();
                valueRow.setColumn(3, new SQLVarchar(fileName));
                if (insertOperation.isAboveFailThreshold(numBadRecords)) {
                    throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                }
            }
        }

        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(valueRow), 1));
    }

    /**
     * Output cut points to files
     * @param cutPointsList
     * @throws IOException
     */
    private void dumpCutPoints(List<Tuple2<Long, byte[][]>> cutPointsList) throws StandardException {

        BufferedWriter br = null;
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);

            for (Tuple2<Long, byte[][]> t : cutPointsList) {
                Long conglomId = t._1;

                Path path = new Path(bulkImportDirectory, conglomId.toString());
                FSDataOutputStream os = fs.create(new Path(path, "cutpoints"));
                br = new BufferedWriter( new OutputStreamWriter( os, "UTF-8" ) );

                byte[][] cutPoints = t._2;

                for (byte[] cutPoint : cutPoints) {
                    br.write(Bytes.toStringBinary(cutPoint) + "\n");
                }
                br.close();
            }
        }catch (IOException e) {
            throw StandardException.plainWrapException(e);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (IOException e) {
                throw StandardException.plainWrapException(e);
            }
        }
    }



    /**
     * Calculate cut points according to statistics. Number of cut points is decided by max region size.
     * @param statistics
     * @return
     * @throws StandardException
     */
    private List<Tuple2<Long, byte[][]>> getCutPoints(double sampleFraction,
            List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> statistics) throws StandardException{
        Map<Long, Tuple2<Double, ColumnStatisticsImpl>> mergedStatistics = mergeResults(statistics);
        List<Tuple2<Long, byte[][]>> result = Lists.newArrayList();

        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        long maxRegionSize = sConfiguration.getRegionMaxFileSize()/2;

        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "maxRegionSize = %d", maxRegionSize);

        // determine how many regions the table/index should be split into
        Map<Long, Integer> numPartitions = new HashMap<>();
        for (Map.Entry<Long, Tuple2<Double, ColumnStatisticsImpl>> longTuple2Entry : mergedStatistics.entrySet()) {
            Tuple2<Double, ColumnStatisticsImpl> stats = longTuple2Entry.getValue();
            double size = stats._1;
            int numPartition = (int)(size/sampleFraction/(1.0*maxRegionSize)) + 1;

            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "total size of the table is %d", size);
            }
            if (numPartition > 1) {
                numPartitions.put(longTuple2Entry.getKey(), numPartition);
            }
        }

        // calculate cut points for each table/index using histogram
        for (Map.Entry<Long, Integer> longIntegerEntry : numPartitions.entrySet()) {
            int numPartition = longIntegerEntry.getValue();
            byte[][] cutPoints = new byte[numPartition-1][];

            ColumnStatisticsImpl columnStatistics = mergedStatistics.get(longIntegerEntry.getKey())._2;
            ItemsSketch itemsSketch = columnStatistics.getQuantilesSketch();
            for (int i = 1; i < numPartition; ++i) {
                SQLBlob blob = (SQLBlob) itemsSketch.getQuantile(i*1.0d/(double)numPartition);
                cutPoints[i-1] = blob.getBytes();
            }
            Tuple2<Long, byte[][]> tuple = new Tuple2<>(longIntegerEntry.getKey(), cutPoints);
            result.add(tuple);
        }
        return result;
    }

    /**
     * Split a table using cut points
     * @param cutPointsList
     * @throws StandardException
     */
    private void splitTables(List<Tuple2<Long, byte[][]>> cutPointsList) throws StandardException {
        SIDriver driver=SIDriver.driver();
        try(PartitionAdmin pa = driver.getTableFactory().getAdmin()){
            for (Tuple2<Long, byte[][]> tuple : cutPointsList) {
                String table = tuple._1.toString();
                byte[][] cutpoints = tuple._2;
                if (LOG.isDebugEnabled()) {
                    SpliceLogUtils.debug(LOG, "split keys for table %s", table);
                    for(byte[] cutpoint : cutpoints) {
                        SpliceLogUtils.debug(LOG, "%s", Bytes.toHex(cutpoint));
                    }
                }
                pa.splitTable(table, cutpoints);
            }
        }
        catch (IOException e) {
            e.printStackTrace();
            throw StandardException.plainWrapException(e);
        }
    }

    /**
     * Merge statistics from each RDD partition
     * @param tuples
     * @return
     * @throws StandardException
     */
    private Map<Long, Tuple2<Double, ColumnStatisticsImpl>> mergeResults(
            List<Tuple2<Long, Tuple2<Double, ColumnStatisticsImpl>>> tuples) throws StandardException{

        Map<Long, ColumnStatisticsMerge> sm = new HashMap<>();
        Map<Long, Double> sizeMap = new HashMap<>();

        for (Tuple2<Long,Tuple2<Double, ColumnStatisticsImpl>> t : tuples) {
            Long conglomId = t._1;
            Double size = t._2._1;
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "conglomerate=%d, size=%d", conglomId, size);
            }
            // Merge statistics for keys
            ColumnStatisticsImpl cs = t._2._2;
            ColumnStatisticsMerge columnStatisticsMerge = sm.get(conglomId);
            if (columnStatisticsMerge == null) {
                columnStatisticsMerge = new ColumnStatisticsMerge();
                sm.put(conglomId, columnStatisticsMerge);
            }
            columnStatisticsMerge.accumulate(cs);

            // merge key/value size from all partition
            Double totalSize = sizeMap.get(conglomId);
            if (totalSize == null)
                totalSize = new Double(0);
            totalSize += size;
            if (LOG.isDebugEnabled()) {
                SpliceLogUtils.debug(LOG, "totalSize=%s", totalSize);
            }
            sizeMap.put(conglomId, totalSize);
        }

        Map<Long, Tuple2<Double, ColumnStatisticsImpl>> statisticsMap = new HashMap<>();
        for (Map.Entry<Long, ColumnStatisticsMerge> longColumnStatisticsMergeEntry : sm.entrySet()) {
            Double totalSize = sizeMap.get(longColumnStatisticsMergeEntry.getKey());
            ColumnStatisticsImpl columnStatistics = longColumnStatisticsMergeEntry.getValue().terminate();
            statisticsMap.put(longColumnStatisticsMergeEntry.getKey(), new Tuple2(totalSize, columnStatistics));
        }

        return statisticsMap;
    }
}
