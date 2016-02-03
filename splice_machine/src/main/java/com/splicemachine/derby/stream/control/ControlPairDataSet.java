package com.splicemachine.derby.stream.control;

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.*;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.DataSetWriter;
import com.splicemachine.derby.stream.output.DataSetWriterBuilder;
import com.splicemachine.derby.stream.output.InsertDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.UpdateDataSetWriterBuilder;
import com.splicemachine.derby.stream.output.delete.DeletePipelineWriter;
import com.splicemachine.derby.stream.output.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.output.direct.DirectDataSetWriter;
import com.splicemachine.derby.stream.output.direct.DirectPipelineWriter;
import com.splicemachine.derby.stream.output.direct.DirectTableWriterBuilder;
import com.splicemachine.derby.stream.output.insert.InsertPipelineWriter;
import com.splicemachine.derby.stream.output.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.output.update.UpdatePipelineWriter;
import com.splicemachine.derby.stream.output.update.UpdateTableWriterBuilder;
import com.splicemachine.kvpair.KVPair;

import scala.Tuple2;

import javax.annotation.Nullable;
import java.util.*;

import static com.splicemachine.derby.stream.control.ControlUtils.entryToTuple;
import static com.splicemachine.derby.stream.control.ControlUtils.multimapFromIterable;

/**
 *
 *
 * @see com.google.common.collect.Multimap
 * @see com.google.common.collect.Multimaps
 * @see com.google.common.collect.Iterables
 *
 */
public class ControlPairDataSet<K,V> implements PairDataSet<K,V> {
    public Iterable<Tuple2<K,V>> source;
    public ControlPairDataSet(Iterable<Tuple2<K,V>> source) {
        this.source = source;
    }



    @Override
    public DataSet<V> values() {
        return new ControlDataSet<>(FluentIterable.from(source).transform(new Function<Tuple2<K,V>, V>() {
            @Nullable
            @Override
            public V apply(@Nullable Tuple2<K,V>t) {
                return t._2();
            }
        }));
    }

    @Override
    public DataSet<V> values(String name) {
        return values();
    }

    @Override
    public DataSet<V> values(boolean isLast) {
        return values();
    }

    @Override
    public DataSet<V> values(String name, boolean isLast, OperationContext context, boolean pushScope, String scopeDetails) {
        return values();
    }

    @Override
    public DataSet<K> keys() {
        return new ControlDataSet<>(FluentIterable.from(source).transform(new Function<Tuple2<K, V>, K>() {
            @Nullable
            @Override
            public K apply(@Nullable Tuple2<K, V> t) {
                return t._1();
            }
        }));
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2) {
        Multimap<K,V> newMap = multimapFromIterable(source);
        return new ControlPairDataSet<K,V>(entryToTuple(Multimaps.forMap(Maps.transformValues(newMap.asMap(), new Function<Collection<V>, V>() {
            @Override
            public V apply(@Nullable Collection<V> vs) {
                try {
                    V returnValue = null;
                    for (V v : vs) {
                        returnValue = function2.call(returnValue, v);
                    }
                    return returnValue;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        })).entries()));
    }

    @Override
    public <Op extends SpliceOperation> PairDataSet<K, V> reduceByKey(final SpliceFunction2<Op,V, V, V> function2, boolean isLast, boolean pushScope, String scopeDetail) {
        return reduceByKey(function2);
    }
    
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(final SpliceFunction<Op,Tuple2<K, V>, U> function) {
        return new ControlDataSet<U>(FluentIterable.from(source).transform(function));
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator) {
        return new ControlPairDataSet<>(FluentIterable.from(source).toSortedList(new Comparator<Tuple2<K, V>>() {
            @Override
            public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
                return comparator.compare(o1._1(), o2._1());
            }
        }));
    }

    @Override
    public PairDataSet<K, V> sortByKey(final Comparator<K> comparator, String name) {
        // 'name' is not used on control side
        return sortByKey(comparator);
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey() {
        Multimap<K,V> newMap = multimapFromIterable(source);
        return new ControlPairDataSet<>(FluentIterable.from(newMap.asMap().entrySet()).transform(new Function<Map.Entry<K, Collection<V>>, Tuple2<K, Iterable<V>>>() {
            @Nullable
            @Override
            public Tuple2<K, Iterable<V>> apply(@Nullable Map.Entry<K, Collection<V>> e) {
                return new Tuple2<K, Iterable<V>>(e.getKey(), e.getValue());
            }
        }));
    }

    @Override
    public PairDataSet<K, Iterable<V>> groupByKey(String name) {
        return groupByKey();
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, Optional<W>>> hashLeftOuterJoin(final PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet) rightDataSet).source);

        return new ControlPairDataSet<>(FluentIterable.from(source).transformAndConcat(new Function<Tuple2<K, V>, Iterable<Tuple2<K, Tuple2<V, Optional<W>>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<V, Optional<W>>>> apply(@Nullable Tuple2<K, V> t) {
                List<Tuple2<K,Tuple2<V,Optional<W>>>> result = new ArrayList<>();
                K key = t._1();
                V value = t._2();
                if (rightSide.containsKey(key)) {
                    for (W rightValue : rightSide.get(key)) {
                        result.add(new Tuple2<>(key,new Tuple2<>(value,Optional.of(rightValue))));
                    }
                } else
                    result.add(new Tuple2<>(key,new Tuple2<>(value,Optional.<W>absent())));
                return result;
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<Optional<V>, W>> hashRightOuterJoin(PairDataSet< K, W> rightDataSet) {
        // Materializes the left side
        final Multimap<K, V> leftSide = multimapFromIterable(source);


        return new ControlPairDataSet<>(FluentIterable.from(((ControlPairDataSet<K,W>) rightDataSet).source).transformAndConcat(new Function<Tuple2<K, W>, Iterable<Tuple2<K, Tuple2<Optional<V>, W>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<Optional<V>, W>>> apply(@Nullable Tuple2<K, W> t) {
                List<Tuple2<K,Tuple2<Optional<V>,W>>> result = new ArrayList<>();
                K key = t._1();
                W value = t._2();
                if (leftSide.containsKey(key)) {
                    for (V leftValue: leftSide.get(key)) {
                        result.add(new Tuple2<>(key,new Tuple2<>(Optional.of(leftValue),value)));
                    }
                } else
                    result.add(new Tuple2<>(key,new Tuple2<>(Optional.<V>absent(),value)));
                return result;
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet<K,W>) rightDataSet).source);
        return new ControlPairDataSet<>(FluentIterable.from(source).transformAndConcat(new Function<Tuple2<K, V>, Iterable<Tuple2<K, Tuple2<V, W>>>>() {
            @Nullable
            @Override
            public Iterable<Tuple2<K, Tuple2<V, W>>> apply(@Nullable Tuple2<K, V> t) {
                List<Tuple2<K,Tuple2<V,W>>> result = new ArrayList<>();
                K key = t._1();
                V value = t._2();
                for (W rightValue : rightSide.get(key)) {
                    result.add(new Tuple2<>(key,new Tuple2<>(value,rightValue)));
                }
                return result;
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, Tuple2<V, W>> hashJoin(PairDataSet< K, W> rightDataSet, String name) {
        // Ignore name on control side
        return hashJoin(rightDataSet);
    }
    
    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet) {
        // Materializes the right side
        final Multimap<K,W> rightSide = multimapFromIterable(((ControlPairDataSet<K,W>) rightDataSet).source);
        return new ControlPairDataSet<>(FluentIterable.from(source).filter(new Predicate<Tuple2<K, V>>() {
            @Override
            public boolean apply(@Nullable Tuple2<K, V> t) {
                return rightSide.get(t._1()).isEmpty();
            }
        }));
    }

    @Override
    public <W> PairDataSet< K, V> subtractByKey(PairDataSet< K, W> rightDataSet, String name) {
        // Ignore name on control side
        return subtractByKey(rightDataSet);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("ControlPairDataSet [");
        int i = 0;
        for (Tuple2<K,V> entry: source) {
            if (i++ != 0)
                sb.append(",");
            sb.append(entry);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op, Iterator<Tuple2<K, V>>, U> f) {
        try {
            return new ControlDataSet<>(f.call(source.iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function) {
        try {
            Iterable<U> iterable =new ArrayList<>(0);
            for (Tuple2<K, V> entry : source) {
                iterable = Iterables.concat(iterable, function.call(new Tuple2<>(entry._1(),entry._2())));
            }
            return new ControlDataSet<>(iterable);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op, Tuple2<K,V>, U> function, boolean isLast) {
        return flatmap(function);
    }
    
    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet) {
        Multimap<K, V> left = multimapFromIterable(source);
        Multimap<K, W> right = multimapFromIterable(((ControlPairDataSet<K, W>) rightDataSet).source);

        List<Tuple2<K, Tuple2<Iterable<V>, Iterable<W>>>> result = new ArrayList<>();
        for (K key: Sets.union(left.keySet(),right.keySet())){
            Collection<V> vs=left.get(key);
            Collection<W> ws=right.get(key);
            result.add(new Tuple2<>(key,new Tuple2<Iterable<V>, Iterable<W>>(vs,ws)));
        }
        return new ControlPairDataSet<>(result);
    }

    @Override
    public <W> PairDataSet<K, Tuple2<Iterable<V>, Iterable<W>>> cogroup(PairDataSet<K, W> rightDataSet, String name) {
        // Ignore name on control side
        return cogroup(rightDataSet);
    }

    @Override
    public PairDataSet<K, V> union(PairDataSet<K, V> dataSet) {
        return new ControlPairDataSet<>(Iterables.concat(source,((ControlPairDataSet<K,V>)dataSet).source));
    }

    @Override
    public DataSetWriterBuilder deleteData(OperationContext operationContext) throws StandardException{
        return new DeleteTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                DeletePipelineWriter dpw = new DeletePipelineWriter(txn,heapConglom,operationContext);
                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,dpw,operationContext);
            }
        };
    }

    @Override
    public InsertDataSetWriterBuilder insertData(OperationContext operationContext) throws StandardException{
        return new InsertTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null:"Txn is null";
                InsertPipelineWriter ipw = new InsertPipelineWriter(pkCols,
                        tableVersion,
                        execRowDefinition,
                        autoIncrementRowLocationArray,
                        spliceSequences,
                        heapConglom,
                        txn,
                        operationContext,
                        isUpsert);
                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,ipw,operationContext);
            }
        }.operationContext(operationContext);
    }

    @Override
    public UpdateDataSetWriterBuilder updateData(OperationContext operationContext) throws StandardException{
        return new UpdateTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null: "Txn is null";
                UpdatePipelineWriter upw =new UpdatePipelineWriter(heapConglom,
                        formatIds,columnOrdering,pkCols,pkColumns,tableVersion,
                        txn,execRowDefinition,heapList,operationContext);

                return new ControlDataSetWriter<>((ControlPairDataSet<K,ExecRow>)ControlPairDataSet.this,upw,operationContext);
            }
        };
    }

    @Override
    public DataSetWriterBuilder directWriteData() throws StandardException{
        return new DirectTableWriterBuilder(){
            @Override
            public DataSetWriter build() throws StandardException{
                assert txn!=null: "Txn is null";
                DirectPipelineWriter writer = new DirectPipelineWriter(destConglomerate,
                        txn, opCtx,skipIndex);

                return new DirectDataSetWriter<>((ControlPairDataSet<K,KVPair>)ControlPairDataSet.this,writer);
            }
        };
    }

    /*
    Cleanup This code...
     */
//    @Override
//    public DataSet<V> insertData(InsertTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
//    }

//    @Override
//    public DataSet<V> updateData(UpdateTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
//        UpdatePipelineWriter updateTableWriter = null;
////        TxnView txn = null;
//        try {
////            txn = operationContext.getOperation().createChildTransaction(Long.toString(builder.getHeapConglom()).getBytes());
//            builder.txn(txn);
//            operationContext.getOperation().fireBeforeStatementTriggers();
//            updateTableWriter = builder.build();
//            updateTableWriter.open(operationContext.getOperation().getTriggerHandler(),operationContext.getOperation());
//            updateTableWriter.update((Iterator<ExecRow>) values().toLocalIterator());
//            ValueRow valueRow = new ValueRow(1);
//            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
//            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
//        } catch (Exception e) {
////            operationContext.getOperation().rollbackTransaction(txn.getTxnId());
//            throw StandardException.plainWrapException(e);
//        } finally {
//            try {
//                if (updateTableWriter != null)
//                    updateTableWriter.close();
//                operationContext.getOperation().fireAfterStatementTriggers();
////                operationContext.getOperation().commitTransaction(txn.getTxnId());
//            } catch (Exception e) {
////                operationContext.getOperation().rollbackTransaction(txn.getTxnId());
//                throw StandardException.plainWrapException(e);
//            }
//        }
//    }
//
//    @Override
//    public DataSet<V> deleteData(DeleteTableWriterBuilder builder, OperationContext operationContext) throws StandardException {
//        DeletePipelineWriter deleteTableWriter = null;
////        TxnView txn = null;
//        try {
////            txn = operationContext.getOperation().createChildTransaction(Long.toString(builder.getHeapConglom()).getBytes());
//            builder.txn(txn); //TODO -sf- get the proper transaction
//            operationContext.getOperation().fireBeforeStatementTriggers();
//            deleteTableWriter = builder.build();
//            deleteTableWriter.open(operationContext.getOperation().getTriggerHandler(),operationContext.getOperation());
//            deleteTableWriter.delete((Iterator<ExecRow>) values().toLocalIterator());
//            ValueRow valueRow = new ValueRow(1);
//            valueRow.setColumn(1,new SQLInteger((int) operationContext.getRecordsWritten()));
//            return new ControlDataSet(Collections.singletonList(new LocatedRow(valueRow)));
//        } catch (Exception e) {
////            operationContext.getOperation().rollbackTransaction(txn.getTxnId());
//            throw StandardException.plainWrapException(e);
//        } finally {
//            try {
//                if (deleteTableWriter != null)
//                    deleteTableWriter.close();
//                operationContext.getOperation().fireAfterStatementTriggers();
////                operationContext.getOperation().commitTransaction(txn.getTxnId());
//            } catch (Exception e) {
////                operationContext.getOperation().rollbackTransaction(txn.getTxnId());
//                throw StandardException.plainWrapException(e);
//            }
//        }
//    }

}
