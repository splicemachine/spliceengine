package com.splicemachine.derby.stream.control;

import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.types.SQLInteger;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;

import org.apache.hadoop.fs.*;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import org.apache.spark.storage.StorageLevel;
import org.sparkproject.guava.collect.FluentIterable;

import scala.Tuple2;

import javax.annotation.Nullable;

import java.io.*;
import java.util.*;
import java.util.zip.GZIPOutputStream;

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
        List<V> rows = new ArrayList<V>();
        Iterator<V> it = iterable.iterator();
        while (it.hasNext())
            rows.add(it.next());
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
        return new ControlDataSet(Sets.newHashSet(iterable));
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
        StringBuffer controlDataSet = new StringBuffer("ControlDataSet {");
        controlDataSet.append("}");
        return controlDataSet.toString();
    }

    @Override
    public long count() {
        return Iterables.size(iterable);
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet) {
        return new ControlDataSet(Iterables.concat(iterable, ((ControlDataSet) dataSet).iterable));
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
    public DataSet< V> intersect(DataSet< V> dataSet) {
        return new ControlDataSet<>(Sets.intersection(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
    }

    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        return new ControlDataSet<>(Sets.difference(Sets.newHashSet(iterable),Sets.newHashSet(((ControlDataSet) dataSet).iterable)));
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
    public <Op extends SpliceOperation> DataSet<LocatedRow> writeToDisk(String path, SpliceFunction2<Op, OutputStream, Iterator<V>, Integer> exportFunction) {
        Integer count;
        String extension = ".csv";
        ExportOperation op = (ExportOperation) exportFunction.getOperation();
        boolean isCompressed = op.getExportParams().isCompression();
        if (isCompressed) {
            extension += ".gz";
        }
        OutputStream fileOut = null;
        try {
            Path file = new Path(path);
            FileSystem fs = file.getFileSystem(SpliceConstants.config);
            fs.mkdirs(file);
            fileOut = fs.create(new Path(path + "/part-r-00000" + extension), false);
            if (isCompressed) {
                fileOut = new GZIPOutputStream(fileOut);
            }
            count = exportFunction.call(fileOut, iterable.iterator());

        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileOut !=null) {
                try {
                    Closeables.close(fileOut, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

        File success = new File(path + "/_SUCCESS");
        try {
            success.createNewFile();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        ValueRow valueRow = new ValueRow(2);
        valueRow.setColumn(1,new SQLInteger(count));
        valueRow.setColumn(2,new SQLInteger(0));
        return new ControlDataSet<>(Arrays.asList(new LocatedRow(valueRow )));
    }

    @Override
    public void saveAsTextFile(String path) {
        OutputStream fileOut = null;
        try {
            Path file = new Path(path);
            FileSystem fs = file.getFileSystem(SpliceConstants.config);
            fileOut = fs.create(new Path(path), false);
            Iterator iterator = iterable.iterator();
            while (iterator.hasNext()) {
                fileOut.write(iterator.next().toString().getBytes());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (fileOut !=null) {
                try {
                    Closeables.close(fileOut, true);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        }

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
            attributes = new HashMap();
        attributes.put(name,value);
    }

    @Override
    public String getAttribute(String name) {
        if (attributes ==null)
            return null;
        return attributes.get(name);
    }
}
