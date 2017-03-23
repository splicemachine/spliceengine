/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.spark;

import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import com.google.common.collect.Lists;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.types.DataType;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.stream.function.RowToLocatedRowFunction;
import com.splicemachine.derby.vti.SpliceFileVTI;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.DistributedDataSetProcessor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.iapi.ScanSetBuilder;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.mrio.api.core.SMTextInputFormat;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.utils.SpliceLogUtils;
import scala.collection.JavaConverters;

/**
 * Spark-based DataSetProcessor.
 *
 */
public class SparkDataSetProcessor implements DistributedDataSetProcessor, Serializable {
    private long failBadRecordCount = -1;
    private boolean permissive;
    private String statusDirectory;
    private String importFileName;

    private static final Logger LOG = Logger.getLogger(SparkDataSetProcessor.class);

    public SparkDataSetProcessor() {
    }

    @Override
    public Type getType() {
        return Type.SPARK;
    }

    @Override
    public void setup(Activation activation,String description,String schedulerPool) throws StandardException{
        String sql = activation.getPreparedStatement().getSource();
        long txnId = getCurrentTransaction(activation).getTxnId();
        sql = (sql == null) ? description : sql;
        String userId = activation.getLanguageConnectionContext().getCurrentUserId(activation);
        String jobName = userId + " <" + txnId + ">";
        if (LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG, "setup(): jobName = %s", jobName);
        setJobGroup(jobName,sql);
        setSchedulerPool(schedulerPool);
    }

    private static TxnView getCurrentTransaction(Activation activation) throws StandardException {
        TransactionController transactionExecute = activation.getLanguageConnectionContext().getTransactionExecute();
        Transaction rawStoreXact = ((TransactionManager) transactionExecute).getRawStoreXact();
        return ((BaseSpliceTransaction) rawStoreXact).getActiveStateTxn();
    }

    @Override
    public <Op extends SpliceOperation,V> ScanSetBuilder<V> newScanSet(Op spliceOperation,String tableName) throws StandardException{
        return new SparkScanSetBuilder<>(this,tableName,spliceOperation); // tableName = conglomerate number
    }

    @Override
    public <V> DataSet<V> getEmpty() {
        return getEmpty(RDDName.EMPTY_DATA_SET.displayName());
    }

    @Override
    public <V> DataSet<V> getEmpty(String name) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1), name);
    }

    @Override
    public <V> DataSet<V> singleRowDataSet(V value) {
        return singleRowDataSet(value, "Finalize Result");
    }

    @SuppressWarnings({ "unchecked" })
    @Override
    public <V> DataSet<V> singleRowDataSet(V value, Object caller) {
        String scope = StreamUtils.getScopeString(caller);
        SpliceSpark.pushScope(scope);
        try {
            JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Collections.singletonList(value), 1);
            rdd1.setName(RDDName.SINGLE_ROW_DATA_SET.displayName());
            return new SparkDataSet<>(rdd1);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Op spliceOperation) {
        setupBroadcastedActivation(spliceOperation.getActivation(), spliceOperation);
        OperationContext<Op> operationContext =new SparkOperationContext<>(spliceOperation,broadcastedActivation.get());
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive(statusDirectory, importFileName, failBadRecordCount);
        }
        return operationContext;
    }


    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        if (activation !=null) {
            return new SparkOperationContext<>(activation, broadcastedActivation.get());
        } else {
            return new SparkOperationContext<>(activation, null);
        }
    }

    @Override
    public void setJobGroup(String jobName, String jobDescription) {
        if (LOG.isTraceEnabled())
            LOG.trace(String.format("setJobGroup(): jobName=%s, jobDescription=%s", jobName, jobDescription));
        SpliceSpark.getContext().setJobGroup(jobName, jobDescription);
    }

    public void setSchedulerPool(String pool) {
        SpliceSpark.getContext().setLocalProperty("spark.scheduler.pool",pool);
    }

    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path) throws StandardException {
        return readWholeTextFile(path,null);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public PairDataSet<String, InputStream> readWholeTextFile(String path, SpliceOperation op) throws StandardException {
        try {
            FileInfo fileInfo = ImportUtils.getImportFileInfo(path);
            String displayString="";
            if(op!=null)
                displayString = op.getScopeName()+": "+OperationContext.Scope.READ_TEXT_FILE.displayName();
            SpliceSpark.pushScope(displayString);
            JavaPairRDD rdd = SpliceSpark.getContext().newAPIHadoopFile(
                    path,
                    WholeTextInputFormat.class,
                    String.class,
                    InputStream.class,
                    HConfiguration.unwrapDelegate());
            return new SparkPairDataSet<>(rdd,OperationContext.Scope.READ_TEXT_FILE.displayName());
        } catch (IOException | StandardException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public DataSet<String> readTextFile(String path) throws StandardException {
        return readTextFile(path, null);
    }
    
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public DataSet<String> readTextFile(String path, SpliceOperation op) throws StandardException {
        try {
            FileInfo fileInfo = ImportUtils.getImportFileInfo(path);
            String displayString="";
            if(op!=null)
                displayString = op.getScopeName()+": "+OperationContext.Scope.READ_TEXT_FILE.displayName();

            SpliceSpark.pushScope(displayString);
            JavaPairRDD<LongWritable, Text> pairRdd=SpliceSpark.getContext().newAPIHadoopFile(
                    path,
                    SMTextInputFormat.class,
                    LongWritable.class,
                    Text.class,
                    new Configuration(HConfiguration.unwrapDelegate()));

            JavaRDD rdd =pairRdd.values()
                    .map(new Function<Text,String>() {
                        @Override
                        public String call(Text o) throws Exception {
                            return o.toString();
                        }
                    });
            SparkUtils.setAncestorRDDNames(rdd, 1, new String[] {fileInfo.toSummary()}, null);
            return new SparkDataSet<>(rdd,OperationContext.Scope.READ_TEXT_FILE.displayName());
        } catch (IOException | StandardException ioe) {
            throw new RuntimeException(ioe);
        } finally {
            SpliceSpark.popScope();
        }
    }

    @Override
    public <K, V> PairDataSet<K, V> getEmptyPair() {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(Collections.<Tuple2<K,V>>emptyList(), 1));
    }

    @Override
    public <V> DataSet< V> createDataSet(Iterator<V> value) {
        return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Lists.newArrayList(value)));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet< V> createDataSet(Iterator<V> value, String name) {
        JavaRDD rdd1 = SpliceSpark.getContext().parallelize(Lists.newArrayList(value));
        rdd1.setName(name);
        return new SparkDataSet(rdd1);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <K, V> PairDataSet<K, V> singleRowPairDataSet(K key, V value) {
        return new SparkPairDataSet<>(SpliceSpark.getContext().parallelizePairs(Arrays.<Tuple2<K, V>>asList(new Tuple2(key, value)), 1));
    }

    @Override
    public void setPermissive(String statusDirectory, String importFileName, long badRecordThreshold) {
        this.permissive = true;
        this.statusDirectory = statusDirectory;
        this.importFileName = importFileName;
        this.failBadRecordCount = badRecordThreshold;
    }


    @Override
    public void clearBroadcastedOperation(){
        broadcastedActivation.remove();
    }

    @Override
    public void stopJobGroup(String jobName) {
        SpliceSpark.getContext().cancelJobGroup(jobName);
    }

    private transient ThreadLocal<BroadcastedActivation> broadcastedActivation = new ThreadLocal<>();

    private void setupBroadcastedActivation(Activation activation, SpliceOperation root){
        if(broadcastedActivation.get()==null){
            broadcastedActivation.set(new BroadcastedActivation(activation, root));
        }
    }

    @Override
    public Partitioner getPartitioner(DataSet<LocatedRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return new HBasePartitioner(dataSet, template, keyDecodingMap, keyOrder, rightHashKeys);
    }


    @Override
    public <V> DataSet<V> readParquetFile(int[] baseColumnMap, String location,
                                          OperationContext context, Qualifier[][] qualifiers,
                                          DataValueDescriptor probeValue,  ExecRow execRow) throws StandardException {
        try {
            Dataset<Row> table = null;
            try {
                table = SpliceSpark.getSession().read().parquet(location);
            } catch (Exception e) {
                return handleExceptionInCaseOfEmptySet(e,location);
            }
            table = processExternalDataset(table,baseColumnMap,qualifiers,probeValue);
            return new SparkDataSet(table
                    .rdd().toJavaRDD()
                    .map(new RowToLocatedRowFunction(context,execRow)));
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE,e.getMessage());
        }
    }

    /**
     *
     * Spark cannot handle empty directories.  Unfortunately, sometimes tables are empty.  Returns
     * empty when it can infer the error and check the directory for files.
     *
     * @param e
     * @param <V>
     * @return
     * @throws Exception
     */
    private <V> DataSet<V> handleExceptionInCaseOfEmptySet(Exception e, String location) throws Exception {
        // Cannot Infer Schema, Argh
        if (e instanceof AnalysisException && e.getMessage() != null && e.getMessage().startsWith("Unable to infer schema")) {
            // Lets check if there are existing files...
            FileInfo fileInfo = ImportUtils.getImportFileInfo(location);
            if (fileInfo.fileCount() == 0) // Handle Empty Directory
                return getEmpty();
        }
        throw e;
    }

    @Override
    public StructType getExternalFileSchema(String storedAs, String location){
        StructType schema = null;
        if (storedAs!=null) {
            if (storedAs.toLowerCase().equals("p")) {
                schema =  SpliceSpark.getSession().read().parquet(location).schema();

            }
            if (storedAs.toLowerCase().equals("o")) {
                schema =  SpliceSpark.getSession().read().orc(location).schema();

            }
            if (storedAs.toLowerCase().equals("t")) {
                schema =  SpliceSpark.getSession().read().csv(location).schema();
            }
        }

        return schema;
    }

    @Override
    public void createEmptyExternalFile(ExecRow execRows, int[] baseColumnMap, int[] partitionBy, String storedAs,  String location, String compression) throws StandardException {
        try{


            Dataset<Row> empty =  SpliceSpark.getSession()
                    .createDataFrame(new ArrayList<Row>(), execRows.schema());

            List<Column> cols = new ArrayList();
            for (int i = 0; i < baseColumnMap.length; i++) {
                cols.add(new Column(ValueRow.getNamedColumn(baseColumnMap[i])));
            }
            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(ValueRow.getNamedColumn(partitionBy[i]));
            }
                if (storedAs!=null) {
                    if (storedAs.toLowerCase().equals("p")) {
                        empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                                .mode(SaveMode.Append).parquet(location);

                    }
                    if (storedAs.toLowerCase().equals("o")) {
                        empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                                .mode(SaveMode.Append).orc(location);

                    }
                    if (storedAs.toLowerCase().equals("t")) {
                        empty.write().option("compression",compression).mode(SaveMode.Append).csv(location);
                    }
            }


        }

        catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE,e.getMessage());
        }
    }



    @Override
    public void dropPinnedTable(long conglomerateId) throws StandardException {
        if (SpliceSpark.getSession().catalog().isCached("SPLICE_"+conglomerateId)) {
            SpliceSpark.getSession().catalog().uncacheTable("SPLICE_"+conglomerateId);
            SpliceSpark.getSession().catalog().dropTempView("SPLICE_"+conglomerateId);

        }
    }

    private Dataset<Row> processExternalDataset(Dataset<Row> rawDataset,int[] baseColumnMap,Qualifier[][] qualifiers,DataValueDescriptor probeValue) throws StandardException {
        String[] allCols = rawDataset.columns();
        List<Column> cols = new ArrayList();
        for (int i = 0; i < baseColumnMap.length; i++) {
            if (baseColumnMap[i] != -1)
                cols.add(new Column(allCols[i]));
        }
        Dataset dataset = rawDataset
                .select(cols.toArray(new Column[cols.size()]));
        if (qualifiers !=null) {
            Column filter = createFilterCondition(dataset,allCols, qualifiers, baseColumnMap, probeValue);
            if (filter != null)
                dataset = dataset.filter(filter);
        }
        return dataset;

    }
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet<V> readPinnedTable(long conglomerateId, int[] baseColumnMap, String location, OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        try {
            Dataset<Row> table = SpliceSpark.getSession().table("SPLICE_"+conglomerateId);
            table = processExternalDataset(table,baseColumnMap,qualifiers,probeValue);
            return new SparkDataSet(table
                    .rdd().toJavaRDD()
                    .map(new RowToLocatedRowFunction(context,execRow)));
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.PIN_READ_FAILURE,e.getMessage());
        }
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet<V> readORCFile(int[] baseColumnMap, String location,
                                          OperationContext context, Qualifier[][] qualifiers,
                                      DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        try {
            Dataset<Row> table = null;
            try {
                table = SpliceSpark.getSession().read().orc(location);
            } catch (Exception e) {
                return handleExceptionInCaseOfEmptySet(e,location);
            }
            table = processExternalDataset(table,baseColumnMap,qualifiers,probeValue);
            return new SparkDataSet(table
                    .rdd().toJavaRDD()
                    .map(new RowToLocatedRowFunction(context,execRow)));
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE,e.getMessage());
        }
    }

    @Override
    public <V> DataSet<LocatedRow> readTextFile(SpliceOperation op, String location, String characterDelimiter, String columnDelimiter, int[] baseColumnMap,
                                      OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        try {
            Dataset<Row> table = null;
            try {
                table = SpliceSpark.getSession().read().csv(location);

                //1. spark cvs assume all the columns to be string by default
                //   we know the schema, no need to use infer schema which is doing a full scan
                //   so 2 scan , not cheap.
                //2. There is a annoying underscore with csv in columns names
                List<Column> correctSchemaSelection = Arrays.stream(execRow.schema().fields())
                        .map( field -> new Column("_"+field.name()).cast(field.dataType())).collect(Collectors.toList());
                table = table.select(correctSchemaSelection.toArray(new Column[correctSchemaSelection.size()]));

            } catch (Exception e) {
                return handleExceptionInCaseOfEmptySet(e,location);
            }
            table = processExternalDataset(table,baseColumnMap,qualifiers,probeValue);
            return new SparkDataSet(table
                    .rdd().toJavaRDD()
                    .map(new RowToLocatedRowFunction(context,execRow)));
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE,e.getMessage());
        }
    }


    private static Column createFilterCondition(Dataset dataset,String[] allColIdInSpark, Qualifier[][] qual_list, int[] baseColumnMap, DataValueDescriptor probeValue) throws StandardException {
        assert qual_list!=null:"qualifier[][] passed in is null";
        boolean     row_qualifies = true;
        Column andCols = null;
        for (int i = 0; i < qual_list[0].length; i++) {
            Qualifier q = qual_list[0][i];
            if (q.getVariantType() == Qualifier.VARIANT)
                continue; // Cannot Push Down Qualifier
            Column col = dataset.col(allColIdInSpark[q.getStoragePosition()]);
            q.clearOrderableCache();
            Object value = (probeValue==null || i!=0?q.getOrderable():probeValue).getObject();
            switch (q.getOperator()) {
                case DataType.ORDER_OP_LESSTHAN:
                    col=q.negateCompareResult()?col.geq(value):col.lt(value);
                    break;
                case DataType.ORDER_OP_LESSOREQUALS:
                    col=q.negateCompareResult()?col.gt(value):col.leq(value);
                    break;
                case DataType.ORDER_OP_GREATERTHAN:
                    col=q.negateCompareResult()?col.leq(value):col.gt(value);
                    break;
                case DataType.ORDER_OP_GREATEROREQUALS:
                    col=q.negateCompareResult()?col.lt(value):col.geq(value);
                    break;
                case DataType.ORDER_OP_EQUALS:
                    if (value == null) // Handle Null Case, push down into Catalyst and Hopefully Parquet/ORC
                        col=q.negateCompareResult()?col.isNotNull():col.isNull();
                    else
                        col=q.negateCompareResult()?col.notEqual(value):col.equalTo(value);
                    break;
            }
            if (andCols ==null)
                andCols = col;
            else
                andCols = andCols.and(col);
        }
        // all the qual[0] and terms passed, now process the OR clauses
        for (int and_idx = 1; and_idx < qual_list.length; and_idx++) {
            Column orCols = null;
            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++) {
                Qualifier q = qual_list[and_idx][or_idx];
                if (q.getVariantType() == Qualifier.VARIANT)
                    continue; // Cannot Push Down Qualifier
                q.clearOrderableCache();
                Column orCol = dataset.col(allColIdInSpark[(baseColumnMap != null ? baseColumnMap[q.getStoragePosition()] : q.getStoragePosition())]);
                Object value = q.getOrderable().getObject();
                switch (q.getOperator()) {
                    case DataType.ORDER_OP_LESSTHAN:
                        orCol = q.negateCompareResult() ? orCol.geq(value) : orCol.lt(value);
                        break;
                    case DataType.ORDER_OP_LESSOREQUALS:
                        orCol = q.negateCompareResult() ? orCol.gt(value) : orCol.leq(value);
                        break;
                    case DataType.ORDER_OP_GREATERTHAN:
                        orCol = q.negateCompareResult() ? orCol.leq(value) : orCol.gt(value);
                        break;
                    case DataType.ORDER_OP_GREATEROREQUALS:
                        orCol = q.negateCompareResult() ? orCol.lt(value) : orCol.geq(value);
                        break;
                    case DataType.ORDER_OP_EQUALS:
                        orCol = q.negateCompareResult() ? orCol.notEqual(value) : orCol.equalTo(value);
                        break;
                }
                if (orCols == null)
                    orCols = orCol;
                else
                    orCols = orCols.or(orCol);
            }
            if (orCols!=null) {
                if (andCols ==null)
                    andCols = orCols;
                else
                    andCols = andCols.and(orCols);
            }
        }
        return andCols;
    }

    @Override
    public void refreshTable(String location) {
        SpliceSpark.getSession().catalog().refreshByPath(location);
    }
}
