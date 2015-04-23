package com.splicemachine.derby.stream;

import com.google.common.base.Optional;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFunction;
import com.splicemachine.derby.stream.function.SpliceFunction2;
import scala.Tuple2;

import java.util.Comparator;

/**
 * Created by jleach on 4/13/15.
 */
public interface PairDataSet<Op extends SpliceOperation,K,V> {
    public DataSet<Op,V> values();
    public DataSet<Op,K> keys();
    public PairDataSet<Op,K,V> reduceByKey(SpliceFunction2<Op,V,V,V> function2);
    public <U> DataSet<Op,U> map(SpliceFunction<Op,Tuple2<K,V>,U> function);
    public PairDataSet<Op,K,V> sortByKey(Comparator<K> comparator);
    public <W> PairDataSet<Op,K,Tuple2<V,Optional<W>>> hashLeftOuterJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,Tuple2<Optional<V>,W>> hashRightOuterJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,Tuple2<V,W>> hashJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,Tuple2<V,Optional<W>>> broadcastLeftOuterJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,Tuple2<Optional<V>,W>> broadcastRightOuterJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,Tuple2<V,W>> broadcastJoin(PairDataSet<Op,K,W> rightDataSet);
    public <W> PairDataSet<Op,K,V> subtract(PairDataSet<Op,K,W> rightDataSet);
    public String toString();

/*    public <W> PairDataSet<Op,K,Tuple2<V,Optional<W>>> nestedLoopLeftOuterJoin(PairDataSet<Op,K,W> rightDataSet); ?
    public <W> PairDataSet<Op,K,Tuple2<Optional<V>,W>> nestedLoopRightOuterJoin(PairDataSet<Op,K,W> rightDataSet); ?
    public <W> PairDataSet<Op,K,Tuple2<V,W>> nestedLoopJoin(PairDataSet<Op,K,W> rightDataSet); ?
*/
}