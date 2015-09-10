package com.splicemachine.derby.stream.spark;

import com.clearspring.analytics.util.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.load.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.TableScanOperation;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.function.TableScanTupleFunction;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iterator.TableScannerIterator;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.api.core.MultiRegionRemoteScanner;
import com.splicemachine.mrio.api.core.SMInputFormat;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.impl.TransactionalRegions;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;

/**
 * Created by jleach on 4/13/15.
 */
public class SparkDataSetProcessor implements DataSetProcessor, Serializable {

    public SparkDataSetProcessor() {

    }

    @Override
    public <Op extends SpliceOperation, V> DataSet<V> getTableScanner(final Op spliceOperation, TableScannerBuilder siTableBuilder, String tableName) throws StandardException {
        JavaSparkContext ctx = SpliceSpark.getContext();
        Configuration conf = new Configuration(SIConstants.config);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_INPUT_CONGLOMERATE, tableName);
        conf.set(com.splicemachine.mrio.MRConstants.SPLICE_JDBC_STR, "jdbc:splice://localhost:${ij.connection.port}/splicedb;user=splice;password=admin");
        try {
            conf.set(com.splicemachine.mrio.MRConstants.SPLICE_SCAN_INFO, siTableBuilder.getTableScannerBuilderBase64String());
        } catch (IOException ioe) {
            throw StandardException.unexpectedUserException(ioe);
        }
        JavaPairRDD<RowLocation, ExecRow> rawRDD = ctx.newAPIHadoopRDD(conf, SMInputFormat.class,
                RowLocation.class, ExecRow.class);

        return new SparkDataSet(rawRDD.map(
                new TableScanTupleFunction<Op>(createOperationContext(spliceOperation))));
    }

    @Override
    public <V> DataSet<V> getEmpty() {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1));
    }

    @Override
    public <V> DataSet<V>  singleRowDataSet(V value) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Collections.<V>singletonList(value),1));
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation) {
        return new SparkOperationContext<Op>(spliceOperation);
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
        SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
    }

    @Override
    public PairDataSet<String, InputStream> readTextFile(String path) {
        return new SparkPairDataSet<>(SpliceSpark.getContext().newAPIHadoopFile(
                path, WholeTextInputFormat.class, String.class, InputStream.class, SpliceConstants.config
        ));
    }

    @Override
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(Collections.<Tuple2<K,V>>emptyList(), 1));
    }

    @Override
    public TableScannerIterator getTableScannerIterator(TableScanOperation operation) throws StandardException {
        Scan scan = operation.getNonSIScan();
        HTableInterface htable = SpliceAccessManager.getHTable(operation.getTableName());

        TableScannerBuilder builder = operation.getTableScannerBuilder();
        try {
            SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(scan, htable);
            HRegion hregion = splitRegionScanner.getRegion();
            SimpleMeasuredRegionScanner mrs = new SimpleMeasuredRegionScanner(splitRegionScanner, Metrics.noOpMetricFactory());
            ExecRow template = SMSQLUtil.getExecRow(builder.getExecRowTypeFormatIds());
            builder.tableVersion("2.0").region(TransactionalRegions.get(hregion)).template(template).scanner(mrs).scan(scan).metricFactory(Metrics.noOpMetricFactory());
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }

        return new TableScannerIterator(builder, operation);

    }

    @Override
    public <V> DataSet< V> createDataSet(Iterable<V> value) {
        return new SparkDataSet(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }

    @Override
    public <K, V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(Arrays.<Tuple2<K, V>>asList(new Tuple2(key, value)), 1));
    }

}