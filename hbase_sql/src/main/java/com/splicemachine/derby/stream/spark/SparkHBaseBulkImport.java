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
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.function.HFileGenerator;
import com.splicemachine.derby.stream.function.RowAndIndexGenerator;
import com.splicemachine.derby.stream.function.RowKeyStatisticsFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.HBaseBulkImporter;
import com.splicemachine.pipeline.ErrorState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.protobuf.ProtoUtil;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.SpliceLogUtils;
import com.yahoo.sketches.quantiles.ItemsSketch;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.LoadIncrementalHFiles;
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
import java.net.URI;
import java.util.*;

/**
 * Created by jyuan on 3/14/17.
 */
public class SparkHBaseBulkImport implements HBaseBulkImporter {

    private static final Logger LOG=Logger.getLogger(SparkHBaseBulkImport.class);

    private DataSet dataSet;
    private String tableVersion;
    private int[] pkCols;
    private RowLocation[] autoIncrementRowLocationArray;
    private long heapConglom;
    private ExecRow execRow;
    private SpliceSequence[] spliceSequences;
    private OperationContext operationContext;
    private TxnView txn;
    private String bulkImportDirectory;

    public SparkHBaseBulkImport(){
    }

    public SparkHBaseBulkImport(DataSet dataSet,
                                String tableVersion,
                                int[] pkCols,
                                RowLocation[] autoIncrementRowLocationArray,
                                long heapConglom,
                                ExecRow execRow,
                                SpliceSequence[] spliceSequences,
                                OperationContext operationContext,
                                TxnView txn,
                                String bulkImportDirectory) {
        this.dataSet = dataSet;
        this.tableVersion = tableVersion;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.pkCols = pkCols;
        this.heapConglom = heapConglom;
        this.execRow = execRow;
        this.spliceSequences = spliceSequences;
        this.operationContext = operationContext;
        this.txn = txn;
        this.bulkImportDirectory = bulkImportDirectory;
    }

    /**
     *
     * @return
     * @throws StandardException
     */
    public DataSet<LocatedRow> write() throws StandardException {
        Activation activation = operationContext.getActivation();
        SpliceConglomerate conglomerate = (SpliceConglomerate) ((SpliceTransactionManager) activation.getTransactionController()).findConglomerate(heapConglom);
        DataDictionary dd = activation.getLanguageConnectionContext().getDataDictionary();
        ConglomerateDescriptor cd = dd.getConglomerateDescriptor(heapConglom);
        TableDescriptor td = dd.getTableDescriptor(cd.getTableID());
        ConglomerateDescriptorList list = td.getConglomerateDescriptorList();
        List<Long> allCongloms = Lists.newArrayList();
        allCongloms.add(td.getHeapConglomerateId());
        ArrayList<DDLMessage.TentativeIndex> tentativeIndexList = new ArrayList();
        for (ConglomerateDescriptor searchCD :list) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                        activation.getLanguageConnectionContext(),
                        td.getHeapConglomerateId(), searchCD.getConglomerateNumber(), td, searchCD.getIndexDescriptor());
                tentativeIndexList.add(ddlChange.getTentativeIndex());
                allCongloms.add(searchCD.getConglomerateNumber());
            }
        }

        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        double sampleFraction = sConfiguration.getBulkImportSampleFraction();

        // TODO: an option to skip sampling and statistics collection
        // Sample the data set and collect statistics for key distributions
        DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);
        DataSet sampleRowAndIndexes = sampledDataSet.flatMap(new RowAndIndexGenerator(pkCols,tableVersion,execRow,autoIncrementRowLocationArray,spliceSequences,heapConglom,
                                txn,operationContext,tentativeIndexList));
        
        DataSet keyStatistics = sampleRowAndIndexes.mapPartitions(new RowKeyStatisticsFunction(td.getHeapConglomerateId(), tentativeIndexList));

        List<Tuple2<Long, ColumnStatisticsImpl>> result = keyStatistics.collect();

        // Calculate cut points for main table and index tables
        List<Tuple2<Long, byte[][]>> cutPoints = getCutPoints(result);
        
        // Sampling with statistics (Use Existing byte[] stats SQLBinary)
        // Statistics for each number (partitioned by conglomerate)
        // Re-read the data with cutpoints (every 5%)
        // Sort within partition based on rowkey
        // write to file (check log you will see the logic for HFiles)
        // Perform bulk import of file
        // You will see that in documentation for HBase
        splitTables(cutPoints);
        List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>>  hashBucketKeys =
                getHashBucketKeys(allCongloms, bulkImportDirectory);

        // Read data again
        DataSet rowAndIndexes = dataSet.flatMap(new RowAndIndexGenerator(pkCols,tableVersion,execRow,autoIncrementRowLocationArray,spliceSequences,heapConglom,
                txn,operationContext,tentativeIndexList));
        // Generate HFiles
        DataSet HFileSet = rowAndIndexes.mapPartitions(new HFileGenerator(operationContext, txn.getTxnId(), heapConglom, bulkImportDirectory, hashBucketKeys));
        List<String>  files = HFileSet.collect();

        if (LOG.isDebugEnabled()) {
            SpliceLogUtils.debug(LOG, "created %d HFiles", files.size());
        }

        bulkLoad(hashBucketKeys);

        ValueRow valueRow=new ValueRow(3);
        valueRow.setColumn(1,new SQLLongint(operationContext.getRecordsWritten()));
        valueRow.setColumn(2,new SQLLongint());
        valueRow.setColumn(3,new SQLVarchar());
        InsertOperation insertOperation=((InsertOperation)operationContext.getOperation());
        if(insertOperation!=null && operationContext.isPermissive()) {
            long numBadRecords = operationContext.getBadRecords();
            valueRow.setColumn(2,new SQLLongint(numBadRecords));
            if (numBadRecords > 0) {
                String fileName = operationContext.getStatusDirectory();
                valueRow.setColumn(3,new SQLVarchar(fileName));
                if (insertOperation.isAboveFailThreshold(numBadRecords)) {
                    throw ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException(fileName);
                }
            }
        }
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.singletonList(new LocatedRow(valueRow)), 1));
    }

    private void bulkLoad(List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>> hashBucketKeys) throws StandardException{
        try {
            Configuration conf = HConfiguration.unwrapDelegate();
            LoadIncrementalHFiles loader = new LoadIncrementalHFiles(conf);

            for (Tuple2<Long, List<HFileGenerator.HashBucketKey>> t : hashBucketKeys) {
                Long conglom = t._1;
                List<HFileGenerator.HashBucketKey> l = t._2;
                HTable table = new HTable(conf, TableName.valueOf("splice:" + conglom));
                for (HFileGenerator.HashBucketKey hashBucketKey : l) {
                    Path path = new Path(hashBucketKey.getPath()).getParent();
                    loader.doBulkLoad(path, table);
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "Loaded file %s", path.toString());
                    }
                }
            }
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
    
    public static Path getRandomFilename(final Path dir)
            throws IOException{
        return new Path(dir, java.util.UUID.randomUUID().toString().replaceAll("-",""));
    }

    /**
     * Get actual partition boundaries for each table and index
     * @param congloms
     * @return
     */
    private List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>> getHashBucketKeys(
            List<Long> congloms,
            String bulkImportDirectory) throws StandardException {

        try {
            // Create bulk import dorectory if it does not exist
            Configuration conf = HConfiguration.unwrapDelegate();
            FileSystem fs = FileSystem.get(URI.create(bulkImportDirectory), conf);
            Path bulkImportPath = new Path(bulkImportDirectory);

            List<Tuple2<Long, List<HFileGenerator.HashBucketKey>>> tablePartitionBoundaries = Lists.newArrayList();
            for (Long conglom : congloms) {
                Path tablePath = new Path(bulkImportPath, conglom.toString());
                SIDriver driver = SIDriver.driver();
                try (PartitionAdmin pa = driver.getTableFactory().getAdmin()) {
                    Iterable<? extends Partition> partitions = pa.allPartitions(conglom.toString());
                    List<HFileGenerator.HashBucketKey> b = Lists.newArrayList();
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "partition information for table %d", conglom);
                    }
                    int count = 0;
                    for (Partition partition : partitions) {
                        byte[] startKey = partition.getStartKey();
                        byte[] endKey = partition.getEndKey();
                        Path regionPath = getRandomFilename(tablePath);
                        Path familyPath = new Path(regionPath, "V");
                        b.add(new HFileGenerator.HashBucketKey(conglom, startKey, endKey, familyPath.toString()));
                        count++;
                        if (LOG.isDebugEnabled()) {
                            SpliceLogUtils.debug(LOG, "start key: %s", Bytes.toHex(startKey));
                            SpliceLogUtils.debug(LOG, "end key: %s", Bytes.toHex(endKey));
                            SpliceLogUtils.debug(LOG, "path = %s");
                        }
                    }
                    if (LOG.isDebugEnabled()) {
                        SpliceLogUtils.debug(LOG, "number of partition: %d", count);
                    }
                    tablePartitionBoundaries.add(new Tuple2<>(conglom, b));
                }
            }
            return tablePartitionBoundaries;
        } catch (IOException e) {
            throw StandardException.plainWrapException(e);
        }
    }
    /**
     * Calculate cupoints according to statistics. Number of cut points is configurable.
     * @param statistics
     * @return
     * @throws StandardException
     */
    private List<Tuple2<Long, byte[][]>> getCutPoints(List<Tuple2<Long, ColumnStatisticsImpl>> statistics) throws StandardException{
        Map<Long, ColumnStatisticsImpl> mergedStatistics = mergeResults(statistics);
        List<Tuple2<Long, byte[][]>> result = Lists.newArrayList();

        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        int numPartition = sConfiguration.getBulkImportPartitionCount();

        for (Long conglomId : mergedStatistics.keySet()) {
            byte[][] cutPoints = new byte[numPartition-1][];
            ColumnStatisticsImpl columnStatistics = mergedStatistics.get(conglomId);
            ItemsSketch itemsSketch = columnStatistics.getQuantilesSketch();
            for (int i = 1; i < numPartition; ++i) {
                SQLBlob blob = (SQLBlob) itemsSketch.getQuantile(i*1.0d/(double)numPartition);
                cutPoints[i-1] = blob.getBytes();
            }
            Tuple2<Long, byte[][]> tuple = new Tuple2<>(conglomId, cutPoints);
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
     *
     * @param tuples
     * @return
     * @throws StandardException
     */
    private Map<Long, ColumnStatisticsImpl> mergeResults(List<Tuple2<Long, ColumnStatisticsImpl>> tuples) throws StandardException{
        Map<Long, ColumnStatisticsMerge> sm = new HashMap<>();
        for (Tuple2<Long, ColumnStatisticsImpl> t : tuples) {
             Long conglomId = t._1;
             ColumnStatisticsImpl cs = t._2;
             ColumnStatisticsMerge columnStatisticsMerge = sm.get(conglomId);
             if (columnStatisticsMerge == null) {
                 columnStatisticsMerge = new ColumnStatisticsMerge();
                 sm.put(conglomId, columnStatisticsMerge);
             }
             columnStatisticsMerge.accumulate(cs);
        }

        Map<Long, ColumnStatisticsImpl> statisticsMap = new HashMap<>();
        for (Long conglomId : sm.keySet()) {
            statisticsMap.put(conglomId, sm.get(conglomId).terminate());
        }

        return statisticsMap;
    }
}
