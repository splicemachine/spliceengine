package com.splicemachine.derby.stream.spark;


import com.splicemachine.access.HConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.stream.BaseStreamTest;
import com.splicemachine.derby.stream.spark.fake.FakeOutputFormat;
import com.splicemachine.derby.stream.spark.fake.FakeTableWriterBuilder;
import com.splicemachine.derby.stream.utils.TableWriterUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.PairFunction;
import org.junit.Assert;
import org.junit.Test;
import scala.Tuple2;
import scala.util.Either;

import java.io.*;

public class SMOutputFormatTest extends BaseStreamTest {

    @Test
    public void readExceptionsCauseAbort() throws StandardException, IOException {
        SparkPairDataSet<ExecRow, ExecRow> dataset = new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(tenRows).mapToPair(new FailFunction()));
        JavaPairRDD<ExecRow, Either<Exception, ExecRow>> rdd = dataset.wrapExceptions();

        final Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
        TableWriterUtils.serializeInsertTableWriterBuilder(conf, new FakeTableWriterBuilder(false));
        conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,FakeOutputFormat.class,FakeOutputFormat.class);
        File file = File.createTempFile(SMOutputFormatTest.class.getName(), "exception");
        file.delete();
        file.mkdir();
        conf.set("abort.directory", file.getAbsolutePath());
        try {
            rdd.saveAsNewAPIHadoopDataset(conf);
            Assert.fail("Expected exception");
        } catch (Exception se) {
            Assert.assertTrue("Unexpected exception", se instanceof SparkException);
        }
        File[] files = file.listFiles();
        Assert.assertTrue("Abort() not called", files.length > 0);
    }

    @Test
    public void writeExceptionsCauseAbort() throws StandardException, IOException {
        SparkPairDataSet<RowLocation, ExecRow> dataset = new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(tenRows).mapToPair(new ToRowLocationFunction()));
        JavaPairRDD<RowLocation, Either<Exception, ExecRow>> rdd = dataset.wrapExceptions();

        final Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
        TableWriterUtils.serializeInsertTableWriterBuilder(conf, new FakeTableWriterBuilder(true));
        conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR, FakeOutputFormat.class, FakeOutputFormat.class);
        File file = File.createTempFile(SMOutputFormatTest.class.getName(), "exception");
        file.delete();
        file.mkdir();
        conf.set("abort.directory", file.getAbsolutePath());
        try {
            rdd.saveAsNewAPIHadoopDataset(conf);
            Assert.fail("Expected exception");
        } catch (Exception se) {
            Assert.assertTrue("Unexpected exception", se instanceof SparkException);
        }
        File[] files = file.listFiles();
        Assert.assertTrue("Abort() not called", files.length > 0);
    }

    @Test
    public void abortNotCalled() throws StandardException, IOException {
        SparkPairDataSet<RowLocation, ExecRow> dataset = new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(tenRows).mapToPair(new ToRowLocationFunction()));
        JavaPairRDD<RowLocation, Either<Exception, ExecRow>> rdd = dataset.wrapExceptions();

        final Configuration conf=new Configuration(HConfiguration.unwrapDelegate());
        TableWriterUtils.serializeInsertTableWriterBuilder(conf, new FakeTableWriterBuilder(false));
        conf.setClass(JobContext.OUTPUT_FORMAT_CLASS_ATTR,FakeOutputFormat.class,FakeOutputFormat.class);
        File file = File.createTempFile(SMOutputFormatTest.class.getName(), "noException");
        file.delete();
        file.mkdir();
        conf.set("abort.directory", file.getAbsolutePath());
        rdd.saveAsNewAPIHadoopDataset(conf);
        File[] files = file.listFiles();
        Assert.assertEquals("Abort() was called", 0, files.length);
    }

    public static class FailFunction implements Serializable, PairFunction<Tuple2<ExecRow, ExecRow>, ExecRow, ExecRow> {
        @Override
        public Tuple2<ExecRow, ExecRow> call(Tuple2<ExecRow, ExecRow> execRowExecRowTuple2) throws Exception {
            throw new NullPointerException("fail");
        }
    }

    public static class ToRowLocationFunction implements Serializable, PairFunction<Tuple2<ExecRow, ExecRow>, RowLocation, ExecRow> {
        @Override
        public Tuple2<RowLocation, ExecRow> call(Tuple2<ExecRow, ExecRow> t) throws Exception {
            return new Tuple2<>(null, t._2());
        }
    }
}




