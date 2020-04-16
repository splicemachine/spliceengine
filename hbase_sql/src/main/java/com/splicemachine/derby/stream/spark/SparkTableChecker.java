package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.storage.CheckTableJob.IndexFilter;
import com.splicemachine.derby.impl.storage.CheckTableJob.LeadingIndexColumnInfo;
import com.splicemachine.derby.impl.storage.KeyByRowIdFunction;
import com.splicemachine.derby.stream.function.IndexTransformFunction;
import com.splicemachine.derby.stream.function.KVPairFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created by jyuan on 2/12/18.
 */
public class SparkTableChecker implements TableChecker {

    protected static final Logger LOG=Logger.getLogger(SparkTableChecker.class);

    private long baseTableCount = 0;
    private long indexCount = 0;
    private long invalidIndexCount = 0;
    private long missingIndexCount = 0;
    private String schemaName;
    private String tableName;
    private String indexName;
    private DataSet<ExecRow> baseTable;
    private PairDataSet<String, ExecRow> filteredTable;
    private KeyHashDecoder tableKeyDecoder;
    private ExecRow tableKeyTemplate;
    private KeyHashDecoder indexKeyDecoder;
    private ExecRow indexKeyTemplate;
    private int  maxCheckTableError;
    private long filteredTableCount;
    private TxnView txn;
    private boolean fix;
    private long conglomerate;
    private DDLMessage.TentativeIndex tentativeIndex;

    public SparkTableChecker(String schemaName,
                             String tableName,
                             DataSet table,
                             KeyHashDecoder tableKeyDecoder,
                             ExecRow tableKey,
                             TxnView txn,
                             boolean fix) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.baseTable = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.txn = txn;
        this.fix = fix;
        maxCheckTableError = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   KeyHashDecoder indexKeyDecoder,
                                   ExecRow indexKey,
                                   LeadingIndexColumnInfo leadingIndexColumnInfo,
                                   long conglomerate,
                                   DDLMessage.TentativeIndex tentativeIndex) throws Exception {

        this.indexName = indexName;
        this.indexKeyDecoder = indexKeyDecoder;
        this.indexKeyTemplate = indexKey;
        this.conglomerate = conglomerate;
        this.tentativeIndex = tentativeIndex;

        List<String> messages = Lists.newLinkedList();

        // Count number of rows in base table and reuse it if the index does not exclude default or null keys
        JavaFutureAction<Long> tableCountFuture = null;
        filteredTable = baseTable.filter(new IndexFilter<>(leadingIndexColumnInfo)).index(new KeyByRowIdFunction<>());
        if (baseTableCount == 0 || leadingIndexColumnInfo != null) {
            SpliceSpark.pushScope(String.format("Count table %s.%s", schemaName, tableName));
            tableCountFuture = ((SparkPairDataSet) filteredTable).rdd.countAsync();
            SpliceSpark.popScope();
        }
        // count number of rows in the index
        SpliceSpark.pushScope(String.format("Count index %s.%s", schemaName, indexName));
        JavaFutureAction<Long> indexCountFuture = ((SparkPairDataSet)index).rdd.countAsync();
        SpliceSpark.popScope();

        messages.addAll(checkMissingIndexes(filteredTable, index));
        if (tableCountFuture != null) {
            if (leadingIndexColumnInfo == null) {
                baseTableCount = tableCountFuture.get();
            }
            else {
                filteredTableCount = tableCountFuture.get();
            }
        }

        indexCount = indexCountFuture.get();
        long tableCount = leadingIndexColumnInfo != null ? filteredTableCount : baseTableCount;

        // If index and table count do not match, or there are rows not indexed, check for invalid indexes
        if (indexCount != tableCount ||  missingIndexCount != 0) {
            messages.addAll(checkInvalidIndexes(filteredTable, index));
        }

        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes(index));
        }
        return messages;
    }

    @Override
    public void setTableDataSet(DataSet tableDataSet) {
        this.baseTable = tableDataSet;
    }

    /**
     * Check for duplicate indexes
     * @param index
     * @return
     * @throws StandardException
     * @throws InterruptedException
     * @throws ExecutionException
     */
    private List<String> checkDuplicateIndexes(PairDataSet index) throws StandardException, InterruptedException, ExecutionException {
        List<String> messages = Lists.newLinkedList();
        SpliceSpark.pushScope(String.format("Check duplicates in index %s.%s", schemaName, indexName));
        JavaPairRDD duplicateIndexRdd = ((SparkPairDataSet)index).rdd
                .combineByKey(new createCombiner(), new mergeValue(), new mergeCombiners())
                .filter(new DuplicateIndexFilter());

        long count = 0;
        JavaFutureAction<List> futureAction = duplicateIndexRdd.takeAsync(maxCheckTableError);
        List<Tuple2<String, List<byte[]>>> result = futureAction.get();
        for(Tuple2<String, List<byte[]>> tuple : result) {
            List<byte[]> keys = tuple._2;
            for (byte[] key : keys) {
                indexKeyDecoder.set(key, 0, key.length);
                indexKeyDecoder.decode(indexKeyTemplate);
                messages.add(indexKeyTemplate.getClone().toString() + "=>" + tuple._1);
                count++;
            }
        }
        if (count >= maxCheckTableError) {
            count = duplicateIndexRdd.count();
            messages.add("...");
        }

        messages.add(0, String.format("The following %d indexes are duplicates:", count));
        SpliceSpark.popScope();
        return messages;
    }

    /**
     * Check invalid indexes which do not refer to a base table row
     * @param table
     * @param index
     * @return
     * @throws StandardException
     */
    private List<String> checkInvalidIndexes(PairDataSet table, PairDataSet index) throws StandardException {
        List<String> messages = Lists.newLinkedList();
        SpliceSpark.pushScope(String.format("Check invalidate index from %s.%s", schemaName, indexName));
        PairDataSet<String, byte[]> d1 = index.subtractByKey(table, null);
        SpliceSpark.popScope();
        JavaPairRDD rdd = ((SparkPairDataSet) d1).rdd;
        invalidIndexCount = rdd.count();
        if (invalidIndexCount > 0) {
            if (fix) {
                return fixInvalidIndexes(rdd);
            } else {
                return reportInvalidIndexes(rdd);
            }
        }
        return messages;
    }

    private List<String> fixInvalidIndexes(JavaPairRDD rdd) throws StandardException{
        try {
            List<String> messages = Lists.newLinkedList();
            int i = 0;
            WriteCoordinator writeCoordinator = PipelineDriver.driver().writeCoordinator();
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            Partition indexPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(conglomerate));
            RecordingCallBuffer<KVPair> writeBuffer = writeCoordinator.writeBuffer(indexPartition, txn, null, writeConfiguration);

            messages.add(String.format("The following %d indexes are deleted:", invalidIndexCount));
            Iterator itr = rdd.toLocalIterator();
            while (itr.hasNext()) {
                Tuple2<String, ExecRow> tuple = (Tuple2<String, ExecRow>) itr.next();
                byte[] key = tuple._2.getKey();
                writeBuffer.add(new KVPair(key, new byte[0], KVPair.Type.DELETE));
                if (i == maxCheckTableError) {
                    messages.add("...");
                }
                if (i > maxCheckTableError) {
                    continue;
                }

                indexKeyDecoder.set(key, 0, key.length);
                indexKeyDecoder.decode(indexKeyTemplate);
                messages.add(indexKeyTemplate.getClone().toString() + "=>" + tuple._1);
                i++;
            }
            writeBuffer.flushBuffer();
            return messages;
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }

    private List<String> reportInvalidIndexes(JavaPairRDD rdd) throws StandardException {
        List<String> messages = Lists.newLinkedList();

        int i = 0;
        messages.add(String.format("The following %d indexes are invalid:", invalidIndexCount));
        Iterator itr = rdd.toLocalIterator();
        while (itr.hasNext()) {
            Tuple2<String, byte[]> tuple = (Tuple2<String, byte[]>) itr.next();

            if (i >= maxCheckTableError) {
                messages.add("...");
                break;
            }
            byte[] key = tuple._2;
            indexKeyDecoder.set(key, 0, key.length);
            indexKeyDecoder.decode(indexKeyTemplate);
            messages.add(indexKeyTemplate.getClone().toString() + "=>" + tuple._1);
            i++;
        }

        return  messages;
    }


    /**
     * Check base table whether all rows are indexed
     * @param table
     * @param index
     * @return
     * @throws StandardException
     */
    private List<String> checkMissingIndexes(PairDataSet table, PairDataSet index) throws StandardException {
        List<String> messages = Lists.newLinkedList();
        SpliceSpark.pushScope(String.format("Check unindexed rows from table %s.%s", schemaName, tableName));
        PairDataSet<String, ExecRow> d2 = table.subtractByKey(index, null);

        JavaPairRDD rdd = ((SparkPairDataSet)d2).rdd;
        missingIndexCount = rdd.count();
        if (missingIndexCount > 0) {
            messages = reportMissingIndexes(rdd, fix);
        }

        return  messages;
    }

    public static class AddKeyFunction extends SpliceFunction<SpliceOperation, Tuple2<String, ExecRow>, ExecRow> {
        public AddKeyFunction() {

        }
        @Override
        public ExecRow call(Tuple2<String, ExecRow> tuple) throws Exception {
            byte[] key = Bytes.fromHex(tuple._1);
            tuple._2.setKey(key);
            return tuple._2;
        }
    }

    private List<String> reportMissingIndexes(JavaPairRDD rdd, boolean fix) throws StandardException {

        if (fix) {
            PairDataSet dsToWrite =  new SparkPairDataSet(rdd)
                    .map(new AddKeyFunction())
                    .map(new IndexTransformFunction(tentativeIndex), null, false, true, "Prepare Index")
                    .index(new KVPairFunction(), false, true, "Add missing indexes");
            DataSetWriter writer = dsToWrite.directWriteData()
                    .destConglomerate(tentativeIndex.getIndex().getConglomerate())
                    .txn(txn)
                    .build();
            writer.write();
        }

        List<String> messages = Lists.newLinkedList();
        if (fix) {
            messages.add(String.format("Created indexes for the following %d rows from base table %s.%s:",
                    missingIndexCount, schemaName, tableName));
        }
        else {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:",
                    missingIndexCount, schemaName, tableName));

        }
        int i = 0;
        Iterator itr = rdd.toLocalIterator();
        while (itr.hasNext()) {

            if (i >= maxCheckTableError) {
                messages.add("...");
                break;
            }

            Tuple2<String, ExecRow> tuple = (Tuple2<String, ExecRow>) itr.next();
            if (tableKeyTemplate.nColumns() > 0) {
                byte[] key = Bytes.fromHex(tuple._1);
                tableKeyDecoder.set(key, 0, key.length);
                tableKeyDecoder.decode(tableKeyTemplate);
                messages.add(tableKeyTemplate.getClone().toString());
            }
            else {
                messages.add(tuple._1);
            }

            i++;
        }
        return messages;
    }

    public static class createCombiner implements Function<byte[], List<byte[]>> {

        @Override
        public List<byte[]> call(byte[] key) {

            List<byte[]> l = Lists.newLinkedList();
            l.add(key);
            return l;
        }
    }


    public static class mergeValue implements Function2<List<byte[]>, byte[], List<byte[]>> {

        @Override
        public List<byte[]> call(List<byte[]> l1, byte[] key) {

            l1.add(key);
            return l1;
        }
    }
    public static class mergeCombiners implements Function2<List<byte[]>, List<byte[]>, List<byte[]>> {

        @Override
        public List<byte[]> call(List<byte[]> l1, List<byte[]> l2) {

            l1.addAll(l2);
            return l1;
        }
    }

    public static class DuplicateIndexFilter implements Function<scala.Tuple2<String, List<byte[]>>,Boolean> {

        @Override
        public Boolean call(scala.Tuple2<String, List<byte[]>> tuple2) {
            return tuple2._2.size() > 1;
        }
    }
}
