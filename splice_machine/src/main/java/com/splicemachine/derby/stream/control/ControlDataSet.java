package com.splicemachine.derby.stream.control;

import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.output.ControlExportDataSetWriter;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import scala.Tuple2;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.io.OutputStream;
import java.util.*;

import static com.splicemachine.derby.stream.control.ControlUtils.entryToTuple;

/**
 *
 * Dataset for Client Side Control Processing
 *
 * @see com.splicemachine.derby.stream.iapi.DataSet
 *
 */
public class ControlDataSet<V> implements DataSet<V> {
    protected Iterable<V> iterable;
    protected Map<String,String> attributes;
    public ControlDataSet(Iterable<V> iterable) {
        this.iterable = iterable;
    }

    @Override
    public List<V> collect() {
        List<V> rows =new ArrayList<>();
        for(V anIterable : iterable) rows.add(anIterable);
        return rows;
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        try {
            return new ControlDataSet<>(f.call(FluentIterable.from(iterable).iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast) {
        return mapPartitions(f);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return mapPartitions(f);
    }

    @Override
    public DataSet<V> distinct() {
        return new ControlDataSet<>(Sets.newHashSet(iterable));
    }

    @Override
    public DataSet<V> distinct(String name) {
        return distinct();
    }

    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function) {
        return new ControlPairDataSet<>(FluentIterable.from(iterable).transform(new Function<V, Tuple2<K, U>>() {
            @Nullable
            @Override
            public Tuple2<K, U> apply(@Nullable V v) {
                try {
                    return function.call(v);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }));
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(final SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        return index(function);
    }
    
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new ControlDataSet<U>(Iterables.transform(iterable, function));
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name) {
        return map(function);
    }

    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) {
        return map(function);
    }
    
    @Override
    public Iterator<V> toLocalIterator() {
        return iterable.iterator();
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function) {
        return new ControlPairDataSet<K,V>(entryToTuple(FluentIterable.from(iterable).index(function).entries()));
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(final SpliceFunction<Op, V, K> function, String name) {
        return keyBy(function);
    }

    @Override
    public <Op extends SpliceOperation, K> PairDataSet<K, V> keyBy(
        final SpliceFunction<Op, V, K> function, String name, boolean pushScope, String scopeDetail) {
        return keyBy(function);
    }

    @Override
    public String toString() {
        return "ControlDataSet {}";
    }

    @Override
    public long count() {
        return Iterables.size(iterable);
    }

    @Override
    public DataSet<V> union(DataSet< V> dataSet) {
        return new ControlDataSet<>(Iterables.concat(iterable, ((ControlDataSet<V>) dataSet).iterable));
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet, String name) {
        return union(dataSet);
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new ControlDataSet<>(Iterables.filter(iterable,f));
    }

    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(
        SplicePredicateFunction<Op,V> f, boolean isLast, boolean pushScope, String scopeDetail) {
        return filter(f);
    }

    @Override
    public DataSet< V> intersect(DataSet< V> dataSet) {
        Set<V> left=Sets.newHashSet(iterable);
        Set<V> right=Sets.newHashSet(((ControlDataSet<V>)dataSet).iterable);
        Sets.SetView<V> intersection=Sets.intersection(left,right);
        return new ControlDataSet<>(intersection);
    }

    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        Set<V> left=Sets.newHashSet(iterable);
        Set<V> right=Sets.newHashSet(((ControlDataSet<V>)dataSet).iterable);
        return new ControlDataSet<>(Sets.difference(left,right));
    }

    @Override
    public boolean isEmpty() {
        return Iterables.isEmpty(iterable);
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new ControlDataSet(FluentIterable.from(iterable).transformAndConcat(f));
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
        return flatMap(f);
    }

    @Override
    public <Op extends SpliceOperation,U> DataSet<U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
        return flatMap(f);
    }

    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> f) {
        try {
            return new ControlDataSet<>(f.call(FluentIterable.from(iterable).iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> f, boolean isLast) {
        return offset(f);
    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> f) {
        try {
            return new ControlDataSet<>(f.call(FluentIterable.from(iterable).iterator()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk(){
        return new ControlExportDataSetWriter.Builder<>(this);
    }

    @Override
    public ExportDataSetWriterBuilder saveAsTextFile() {
        return writeToDisk()
                .exportFunction(new SpliceFunction2<SpliceOperation,OutputStream,Iterator<String>,Integer>(){
                    @Override public void writeExternal(ObjectOutput out) throws IOException{ super.writeExternal(out); }
                    @Override public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException{ super.readExternal(in); }
                    @Override public SpliceOperation getOperation(){ return super.getOperation(); }
                    @Override public Activation getActivation(){ return super.getActivation(); }
                    @Override public void prepare(){ super.prepare(); }
                    @Override public void reset(){ super.reset(); }
                    @Override public String getPrettyFunctionName(){ return super.getPrettyFunctionName(); }
                    @Override public String getSparkName(){ return super.getSparkName(); }


                    @Override
                    public Integer call(OutputStream os,Iterator<String> iterator) throws Exception{
                        int size = 0;
                        while(iterator.hasNext()){
                           os.write(iterator.next().getBytes());
                            size++;
                        }
                        return size;
                    }
                });
    }

    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
        return this;
    }

    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle, boolean isLast, OperationContext context, boolean pushScope, String scopeDetail) {
        return this;
    }

    @Override
    public void persist() {
        // no op
    }

    @Override
    public Iterator<V> iterator() {
        return this.toLocalIterator();
    }


    @Override
    public void setAttribute(String name, String value) {
        if (attributes==null)
            attributes = new HashMap<>();
        attributes.put(name,value);
    }

    @Override
    public String getAttribute(String name) {
        if (attributes ==null)
            return null;
        return attributes.get(name);
    }
}
