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
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.impl.store.access.SpliceTransactionManager;
import com.splicemachine.derby.impl.store.access.base.SpliceConglomerate;
import com.splicemachine.derby.stream.function.HFileGenerator;
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
import org.apache.log4j.Logger;
import scala.Tuple2;

import java.io.IOException;
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
                                TxnView txn) {
        this.dataSet = dataSet;
        this.tableVersion = tableVersion;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.pkCols = pkCols;
        this.heapConglom = heapConglom;
        this.execRow = execRow;
        this.spliceSequences = spliceSequences;
        this.operationContext = operationContext;
        this.txn = txn;
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
        ArrayList<DDLMessage.TentativeIndex> tentativeIndexList = new ArrayList();
        for (ConglomerateDescriptor searchCD :list) {
            if (searchCD.isIndex() && !searchCD.isPrimaryKey()) {
                DDLMessage.DDLChange ddlChange = ProtoUtil.createTentativeIndexChange(txn.getTxnId(),
                        activation.getLanguageConnectionContext(),
                        td.getHeapConglomerateId(), searchCD.getConglomerateNumber(), td, searchCD.getIndexDescriptor());
                tentativeIndexList.add(ddlChange.getTentativeIndex());
            }
        }

        SConfiguration sConfiguration = HConfiguration.getConfiguration();
        double sampleFraction = sConfiguration.getBulkImportSampleFraction();

        // Sample the data set and collect statistics for key distributions
        DataSet sampledDataSet = dataSet.sampleWithoutReplacement(sampleFraction);
        DataSet rowAndIndexes = sampledDataSet.flatMap(new HFileGenerator(pkCols,tableVersion,execRow,autoIncrementRowLocationArray,spliceSequences,heapConglom,
                                txn,operationContext,tentativeIndexList));
        
        DataSet keyStatistics = rowAndIndexes.mapPartitions(new RowKeyStatisticsFunction(td.getHeapConglomerateId(), tentativeIndexList));

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

    /**
     *
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
     * 
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
                    SpliceLogUtils.debug(LOG, "split keys for table %d", table);
                    for(byte[] cutpoint : cutpoints) {
                        SpliceLogUtils.debug(LOG, "%s", Bytes.toHex(cutpoint));
                    }
                }
                pa.splitTable(table, cutpoints);

                if (LOG.isDebugEnabled()) {
                    Iterable<? extends Partition> partitions = pa.allPartitions(table);
                    int count = 0;
                    for (Partition partition : partitions) {
                        byte[] startKey = partition.getStartKey();
                        byte[] endKey = partition.getEndKey();

                        SpliceLogUtils.debug(LOG, "start key: %s", Bytes.toHex(startKey));
                        SpliceLogUtils.debug(LOG, "end key: %s", Bytes.toHex(endKey));
                        count++;
                    }
                    SpliceLogUtils.debug(LOG, "Total number of regions: %d", count);
                }
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
