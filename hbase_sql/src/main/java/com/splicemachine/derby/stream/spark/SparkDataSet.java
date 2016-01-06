package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportExecRowWriter;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.stream.function.*;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.output.ExportDataSetWriterBuilder;
import com.splicemachine.utils.ByteDataInput;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.storage.StorageLevel;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.zip.GZIPOutputStream;

/**
 *
 * DataSet Implementation for Spark.
 *
 * @see com.splicemachine.derby.stream.iapi.DataSet
 * @see java.io.Serializable
 *
 */
public class SparkDataSet<V> implements DataSet<V> {
    public JavaRDD<V> rdd;
    private Map<String,String> attributes;
    public SparkDataSet(JavaRDD<V> rdd) {
        this.rdd = rdd;
    }

    public SparkDataSet(JavaRDD<V> rdd, String rddname) {
        this.rdd = rdd;
        if (rdd != null && rddname != null) this.rdd.setName(rddname);
    }

    @Override
    public List<V> collect() {
        return rdd.collect();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f) {
        return new SparkDataSet<>(rdd.mapPartitions(f),f.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, String name) {
        return new SparkDataSet<>(rdd.mapPartitions(f),name);
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet<U> mapPartitions(SpliceFlatMapFunction<Op,Iterator<V>, U> f, boolean isLast) {
        return new SparkDataSet<>(rdd.mapPartitions(f),planIfLast(f,isLast));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> distinct() {
        return distinct("Remove Duplicates");
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public DataSet<V> distinct(String name) {
        JavaRDD rdd1 = rdd.distinct();
        rdd1.setName(name /* MapPartitionsRDD */);
        RDDUtils.setAncestorRDDNames(rdd1, 2, new String[]{"Shuffle Data" /* ShuffledRDD */, "Prepare To Find Distinct" /* MapPartitionsRDD */});
        return new SparkDataSet(rdd1);
    }

    @Override
    public <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2) {
        return rdd.fold(zeroValue,function2);
    }

    @Override
    public <Op extends SpliceOperation> V fold(V zeroValue, SpliceFunction2<Op,V, V, V> function2, boolean isLast) {
        rdd.setName(planIfLast(function2, isLast));
        return rdd.fold(zeroValue, function2);
    }
    
    @Override
    public <Op extends SpliceOperation, K,U> PairDataSet<K,U> index(SplicePairFunction<Op,V,K,U> function) {
       return new SparkPairDataSet(rdd.mapToPair(function), function.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation, K,U>PairDataSet<K, U> index(SplicePairFunction<Op,V,K,U> function, boolean isLast) {
        return new SparkPairDataSet(rdd.mapToPair(function), planIfLast(function, isLast));
    }
    
    @Override
    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function) {
        return new SparkDataSet<>(rdd.map(function), function.getSparkName());
    }

    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, String name) {
        return new SparkDataSet<>(rdd.map(function), name);
    }

    public <Op extends SpliceOperation, U> DataSet<U> map(SpliceFunction<Op,V,U> function, boolean isLast) {
        return new SparkDataSet<>(rdd.map(function), planIfLast(function, isLast));
    }

    @Override
    public Iterator<V> toLocalIterator() {
        return rdd.collect().iterator();
    }

    @SuppressWarnings("rawtypes")
    private String planIfLast(AbstractSpliceFunction f, boolean isLast) {
        if (!isLast) return f.getSparkName();
        String plan = f.getOperation().getPrettyExplainPlan();
        return (plan != null && !plan.isEmpty() ? plan : f.getSparkName());
    }
    
    @Override
    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f) {
        return new SparkPairDataSet(rdd.keyBy(f), f.getSparkName());
    }

    public <Op extends SpliceOperation, K> PairDataSet< K, V> keyBy(SpliceFunction<Op, V, K> f, String name) {
        return new SparkPairDataSet(rdd.keyBy(f), name);
    }
    
    @Override
    public long count() {
        return rdd.count();
    }

    @Override
    public DataSet<V> union(DataSet< V> dataSet) {
        return union(dataSet, SparkConstants.RDD_NAME_UNION);
    }

    @Override
    public DataSet< V> union(DataSet< V> dataSet, String name) {
        JavaRDD rdd1 = rdd.union(((SparkDataSet) dataSet).rdd);
        rdd1.setName(name);
        return new SparkDataSet<>(rdd1);
    }
    
    @Override
    public <Op extends SpliceOperation> DataSet< V> filter(SplicePredicateFunction<Op, V> f) {
        return new SparkDataSet<>(rdd.filter(f), f.getSparkName());
    }

    @Override
    public DataSet< V> intersect(DataSet< V> dataSet) {
        return new SparkDataSet<>(rdd.intersection(((SparkDataSet) dataSet).rdd));
    }

    @Override
    public DataSet< V> subtract(DataSet< V> dataSet) {
        return new SparkDataSet<>(rdd.subtract( ((SparkDataSet) dataSet).rdd));
    }

    @Override
    public boolean isEmpty() {
        return rdd.take(1).isEmpty();
    }

    @Override
    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f) {
        return new SparkDataSet<>(rdd.flatMap(f), f.getSparkName());
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, String name) {
        return new SparkDataSet<>(rdd.flatMap(f), name);
    }

    public <Op extends SpliceOperation, U> DataSet< U> flatMap(SpliceFlatMapFunction<Op, V, U> f, boolean isLast) {
        return new SparkDataSet<>(rdd.flatMap(f), planIfLast(f, isLast));
    }
    
    @Override
    public void close() {

    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction) {
        return new SparkDataSet<>(rdd.mapPartitions(offsetFunction),offsetFunction.getSparkName());
    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> offset(OffsetFunction<Op,V> offsetFunction, boolean isLast) {
        return new SparkDataSet<>(rdd.mapPartitions(offsetFunction),planIfLast(offsetFunction,isLast));
    }

    @Override
    public <Op extends SpliceOperation> DataSet<V> take(TakeFunction<Op,V> takeFunction) {
        JavaRDD<V> rdd1 = rdd.mapPartitions(takeFunction);
        rdd1.setName(takeFunction.getSparkName());
        
        JavaRDD<V> rdd2 = rdd1.coalesce(1, true);
        rdd2.setName("Coalesce 1 partition");
        RDDUtils.setAncestorRDDNames(rdd2, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"});
        
        JavaRDD<V> rdd3 = rdd2.mapPartitions(takeFunction);
        rdd3.setName(takeFunction.getSparkName());
        
        return new SparkDataSet<V>(rdd3);
    }

    @Override
    public ExportDataSetWriterBuilder writeToDisk(){
        return new SparkExportDataSetWriter.Builder();
    }

    public static class EOutputFormat extends FileOutputFormat<Void, LocatedRow> {

        @Override
        public RecordWriter<Void, LocatedRow> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            Configuration conf = taskAttemptContext.getConfiguration();
            String encoded = conf.get("exportFunction");
            ByteDataInput bdi = new ByteDataInput(
            Base64.decodeBase64(encoded));
            SpliceFunction2<ExportOperation, OutputStream, Iterator<LocatedRow>, Void> exportFunction;
            try {
                exportFunction = (SpliceFunction2<ExportOperation, OutputStream, Iterator<LocatedRow>, Void>) bdi.readObject();
            } catch (ClassNotFoundException e) {
                throw new IOException(e);
            }

            final ExportOperation op = exportFunction.getOperation();
            CompressionCodec codec = null;
            String extension = ".csv";
            boolean isCompressed = op.getExportParams().isCompression();
            if (isCompressed) {
                Class<? extends CompressionCodec> codecClass =
                        getOutputCompressorClass(taskAttemptContext, GzipCodec.class);
                codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, conf);
                extension += ".gz";
            }

            Path file = getDefaultWorkFile(taskAttemptContext, extension);
            FileSystem fs = file.getFileSystem(conf);
            OutputStream fileOut = fs.create(file, false);
            if (isCompressed) {
                fileOut = new GZIPOutputStream(fileOut);
            }
            final ExportExecRowWriter rowWriter = ExportFunction.initializeRowWriter(fileOut, op.getExportParams());
            return new RecordWriter<Void, LocatedRow>() {
                @Override
                public void write(Void _, LocatedRow locatedRow) throws IOException, InterruptedException {
                    try {
                        rowWriter.writeRow(locatedRow.getRow(), op.getSourceResultColumnDescriptors());
                    } catch (StandardException e) {
                        throw new IOException(e);
                    }
                }

                @Override
                public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
                    rowWriter.close();
                }
            };
        }
    }

    @Override
    public void saveAsTextFile(String path) {
        rdd.saveAsTextFile(path);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<V> coalesce(int numPartitions, boolean shuffle) {
        JavaRDD rdd1 = rdd.coalesce(numPartitions, shuffle);
        rdd1.setName(String.format("Coalesce %d partitions", numPartitions));
        RDDUtils.setAncestorRDDNames(rdd1, 3, new String[]{"Coalesce Data", "Shuffle Data", "Map For Coalesce"});
        return new SparkDataSet<V>(rdd1);
    }

    @Override
    public void persist() {
        rdd.persist(StorageLevel.MEMORY_AND_DISK_SER_2());
    }

    @Override
    public Iterator<V> iterator() {
        return toLocalIterator();
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
