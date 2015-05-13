package com.splicemachine.derby.stream.iapi;

import com.google.common.base.Optional;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.function.SpliceFlatMapFunction;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import com.splicemachine.derby.stream.function.SplicePairFunction;
import com.splicemachine.derby.stream.temporary.delete.DeleteTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.insert.InsertTableWriterBuilder;
import com.splicemachine.derby.stream.temporary.update.UpdateTableWriterBuilder;
import scala.Tuple2;

import java.util.Comparator;
import java.util.Iterator;

/**
 * Stream of data acting on a key/values.
 */
public interface PairDataSet<K,V> {
    public DataSet<V> values();
    public DataSet<K> keys();
    public <Op extends SpliceOperation> PairDataSet<K,V> reduceByKey(SpliceFunction2<Op,V,V,V> function2);
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,Tuple2<K,V>,U> function);
    public <Op extends SpliceOperation, U> DataSet<U> flatmap(SpliceFlatMapFunction<Op,Tuple2<K,V>,U> function);
    public PairDataSet<K,V> sortByKey(Comparator<K> comparator);
    public PairDataSet<K, Iterable<V>> groupByKey();
    public <W> PairDataSet<K,Tuple2<V,Optional<W>>> hashLeftOuterJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<Optional<V>,W>> hashRightOuterJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<V,W>> hashJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<V,Optional<W>>> broadcastLeftOuterJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<Optional<V>,W>> broadcastRightOuterJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<V,W>> broadcastJoin(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,V> subtractByKey(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,V> broadcastSubtractByKey(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<Iterator<V>, Iterator<W>>> cogroup(PairDataSet<K,W> rightDataSet);
    public <W> PairDataSet<K,Tuple2<Iterator<V>, Iterator<W>>> broadcastCogroup(PairDataSet<K,W> rightDataSet);
    public DataSet<V> insertData(InsertTableWriterBuilder builder);
    public DataSet<V> updateData(UpdateTableWriterBuilder builder);
    public DataSet<V> deleteData(DeleteTableWriterBuilder builder);
    public String toString();


/*    public <W> PairDataSet<K,Tuple2<V,Optional<W>>> nestedLoopLeftOuterJoin(PairDataSet<K,W> rightDataSet); ?
    public <W> PairDataSet<K,Tuple2<Optional<V>,W>> nestedLoopRightOuterJoin(PairDataSet<K,W> rightDataSet); ?
    public <W> PairDataSet<K,Tuple2<V,W>> nestedLoopJoin(PairDataSet<K,W> rightDataSet); ?
*/
}