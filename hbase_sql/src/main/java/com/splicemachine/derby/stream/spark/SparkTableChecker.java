package com.splicemachine.derby.stream.spark;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.TableChecker;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.si.impl.driver.SIDriver;
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

    private long tableCount = 0;
    private long indexCount = 0;
    private long invalidIndexCount = 0;
    private long missingIndexCount = 0;
    private String schemaName;
    private String tableName;
    private String indexName;
    private PairDataSet<String, ExecRow> table;
    private KeyHashDecoder tableKeyDecoder;
    private ExecRow tableKeyTemplate;
    private KeyHashDecoder indexKeyDecoder;
    private ExecRow indexKeyTemplate;
    private int  maxCheckTableError;

    public SparkTableChecker(String schemaName,
                             String tableName,
                             PairDataSet<String, ExecRow> table,
                             KeyHashDecoder tableKeyDecoder,
                             ExecRow tableKey) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.table = table;
        this.tableKeyDecoder = tableKeyDecoder;
        this.tableKeyTemplate = tableKey;
        maxCheckTableError = SIDriver.driver().getConfiguration().getMaxCheckTableErrors();
    }

    @Override
    public List<String> checkIndex(PairDataSet index,
                                   String indexName,
                                   KeyHashDecoder indexKeyDecoder,
                                   ExecRow indexKey) throws Exception {

        this.indexName = indexName;
        this.indexKeyDecoder = indexKeyDecoder;
        this.indexKeyTemplate = indexKey;

        List<String> messages = Lists.newLinkedList();

        //Count number of rows in base table
        JavaFutureAction<Long> tableCountFuture = null;
        if (tableCount == 0) {
            SpliceSpark.pushScope(String.format("Count table %s.%s", schemaName, tableName));
            tableCountFuture = ((SparkPairDataSet) table).rdd.countAsync();
            SpliceSpark.popScope();
        }
        // count number of rows in the index
        SpliceSpark.pushScope(String.format("Count index %s.%s", schemaName, indexName));
        JavaFutureAction<Long> indexCountFuture = ((SparkPairDataSet)index).rdd.countAsync();
        SpliceSpark.popScope();

        messages.addAll(checkMissingIndexes(table, index));
        messages.addAll(checkInvalidIndexes(table, index));

        if (tableCountFuture != null) {
            tableCount = tableCountFuture.get();
        }

        indexCount = indexCountFuture.get();

        if (indexCount - invalidIndexCount > tableCount - missingIndexCount) {
            messages.addAll(checkDuplicateIndexes(index));
        }
        return messages;
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
                .filter(new filter());

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
        JavaPairRDD rdd = ((SparkPairDataSet)d1).rdd;
        invalidIndexCount = rdd.count();
        int i = 0;
        if (invalidIndexCount > 0) {
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
        PairDataSet<String, byte[]> d2 = table.subtractByKey(index, null);

        JavaPairRDD rdd = ((SparkPairDataSet)d2).rdd;
        missingIndexCount = rdd.count();
        int i = 0;
        if (missingIndexCount > 0) {
            messages.add(String.format("The following %d rows from base table %s.%s are not indexed:", missingIndexCount, schemaName, tableName));
            Iterator itr = rdd.toLocalIterator();
            while (itr.hasNext()) {

                if (i >= maxCheckTableError) {
                    messages.add("...");
                    break;
                }

                Tuple2<String, byte[]> tuple = (Tuple2<String, byte[]>) itr.next();
                if (tableKeyTemplate.nColumns() > 0) {
                    byte[] key = tuple._2;
                    tableKeyDecoder.set(key, 0, key.length);
                    tableKeyDecoder.decode(tableKeyTemplate);
                    messages.add(tableKeyTemplate.getClone().toString());
                }
                else {
                    messages.add(tuple._1);
                }

                i++;
            }
        }
        return  messages;
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

    public static class filter implements Function<scala.Tuple2<String, List<byte[]>>,Boolean> {

        @Override
        public Boolean call(scala.Tuple2<String, List<byte[]>> tuple2) {
            return tuple2._2.size() > 1;
        }
    }
}
