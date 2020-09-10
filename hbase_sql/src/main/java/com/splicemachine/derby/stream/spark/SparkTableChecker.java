package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.storage.CheckTableUtils.IndexFilter;
import com.splicemachine.derby.impl.storage.CheckTableUtils.LeadingIndexColumnInfo;
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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
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
    private int  maxCheckTableError;
    private long filteredTableCount;
    private TxnView txn;
    private boolean fix;
    private long conglomerate;
    private DDLMessage.TentativeIndex tentativeIndex;
    private int[] baseColumnMap;
    private boolean isSystemTable;

    @SuppressFBWarnings(value = "EI_EXPOSE_REP2",justification = "Intentional")
    public SparkTableChecker(String schemaName,
                             String tableName,
                             DataSet table,
                             KeyHashDecoder tableKeyDecoder,
                             ExecRow tableKey,
                             TxnView txn,
                             boolean fix,
                             int[] baseColumnMap,
                             boolean isSystemTable) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.baseTable = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        this.txn = txn;
        this.fix = fix;
        this.baseColumnMap = baseColumnMap;
        this.isSystemTable = isSystemTable;
        maxCheckTableError = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   LeadingIndexColumnInfo leadingIndexColumnInfo,
                                   long conglomerate,
                                   DDLMessage.TentativeIndex tentativeIndex) throws Exception {

        this.indexName = indexName;
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
            messages.addAll(checkDuplicateIndexes(filteredTable, index));
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
    private List<String> checkDuplicateIndexes(PairDataSet table, PairDataSet index) throws StandardException {
        try {
            SpliceSpark.pushScope(String.format("Check duplicates in index %s.%s", schemaName, indexName));
            JavaPairRDD duplicateIndexRdd = ((SparkPairDataSet) index).rdd
                    .combineByKey(new CreateCombiner(), new MergeValue(), new MergeCombiners())
                    .filter(new DuplicateIndexFilter());

            JavaPairRDD joinedRdd = duplicateIndexRdd
                    .join(((SparkPairDataSet) table).rdd);

            JavaRDD duplicateIndex = joinedRdd
                    .mapPartitions(new SparkFlatMapFunction<>(new DeleteDuplicateIndexFunction<>(conglomerate, txn, tentativeIndex, baseColumnMap, fix)));

            Iterator it = duplicateIndex.toLocalIterator();
            long count = duplicateIndex.count();
            return reportDuplicateIndexes(it, count, fix);
        }catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
        finally {
            SpliceSpark.popScope();
        }
    }

    private List<String> reportDuplicateIndexes(Iterator it, long count, boolean fix) throws StandardException, InterruptedException, ExecutionException {
        List<String> messages = Lists.newLinkedList();
        int n = 0;
        while (it.hasNext()) {
            n++;
            Tuple2<String, Tuple2<byte[], ExecRow>> t = (Tuple2<String, Tuple2<byte[], ExecRow>>)it.next();
            messages.add(t._2._2 + "@" + Bytes.toHex(t._2._1) + "=>" + t._1);
            if (!fix && n >= maxCheckTableError) {
                messages.add("...");
                break;
            }
        }
        if (fix) {
            messages.add(0,String.format("Deleted the following %d duplicate indexes", count));
        }
        else {
            messages.add(0, String.format("The following %d indexes are duplicates:", count));
        }
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
        PairDataSet<String, Tuple2<byte[], ExecRow>> d1 = index.subtractByKey(table, null);
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
                Tuple2<String, Tuple2<byte[], ExecRow>> tuple = (Tuple2<String, Tuple2<byte[], ExecRow>>) itr.next();
                byte[] key = tuple._2._1;
                writeBuffer.add(new KVPair(key, new byte[0], KVPair.Type.DELETE));
                if (i == maxCheckTableError) {
                    messages.add("...");
                }
                if (i > maxCheckTableError) {
                    continue;
                }
                messages.add(tuple._2._2 + "=>" + tuple._1);
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
            Tuple2<String, Tuple2<byte[], ExecRow>> tuple = (Tuple2<String, Tuple2<byte[], ExecRow>>) itr.next();

            if (i >= maxCheckTableError) {
                messages.add("...");
                break;
            }
            byte[] key = tuple._2._1;
            messages.add(tuple._2._2 + "@" + Bytes.toHex(key) + "=>" + tuple._1);
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
            List<Integer> baseColumnMapList = Lists.newArrayList();
            for (int i = 0; i < baseColumnMap.length; ++i) {
                if (baseColumnMap[i] >= 0) {
                    baseColumnMapList.add(i+1);
                }
            }
            PairDataSet dsToWrite =  new SparkPairDataSet(rdd)
                    .map(new AddKeyFunction())
                    .map(new IndexTransformFunction(tentativeIndex, baseColumnMapList, isSystemTable), null, false, true, "Prepare Index")
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
                ValueRow r = (ValueRow) tableKeyTemplate.getClone();
                messages.add(r.toString());
            }
            else {
                messages.add(tuple._1);
            }

            i++;
        }
        return messages;
    }

    public static class CreateCombiner implements Function<Tuple2<byte[], ExecRow>, List<Tuple2<byte[], ExecRow>>> {

        @Override
        public List<Tuple2<byte[], ExecRow>> call(Tuple2<byte[], ExecRow> t) {

            List<Tuple2<byte[], ExecRow>> l = Lists.newLinkedList();
            l.add(t);
            return l;
        }
    }


    public static class MergeValue implements Function2<List<Tuple2<byte[], ExecRow>>, Tuple2<byte[], ExecRow>, List<Tuple2<byte[], ExecRow>>> {

        @Override
        public List<Tuple2<byte[], ExecRow>> call(List<Tuple2<byte[], ExecRow>> l1, Tuple2<byte[], ExecRow> t) {

            l1.add(t);
            return l1;
        }
    }
    public static class MergeCombiners implements Function2<List<Tuple2<byte[], ExecRow>>, List<Tuple2<byte[], ExecRow>>, List<Tuple2<byte[], ExecRow>>> {

        @Override
        public List<Tuple2<byte[], ExecRow>> call(List<Tuple2<byte[], ExecRow>> l1, List<Tuple2<byte[], ExecRow>> l2) {

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
