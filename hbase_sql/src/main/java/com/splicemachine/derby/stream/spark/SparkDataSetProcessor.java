/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataType;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLVarchar;
import com.splicemachine.db.impl.sql.compile.ExplainNode;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.impl.spark.WholeTextInputFormat;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.stream.function.Partitioner;
import com.splicemachine.derby.stream.function.RowToLocatedRowFunction;
import com.splicemachine.derby.stream.iapi.*;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.derby.stream.utils.StreamUtils;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.mrio.api.core.SMTextInputFormat;
import com.splicemachine.orc.input.SpliceOrcNewInputFormat;
import com.splicemachine.orc.predicate.SpliceORCPredicate;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.system.CsvOptions;
import com.splicemachine.utils.IndentedString;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.io.Externalizable;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.*;

/**
 * Spark-based DataSetProcessor.
 *
 */
public class SparkDataSetProcessor implements DistributedDataSetProcessor, Serializable {
    private static final long serialVersionUID = 9152794997108375878L;
    private long failBadRecordCount = -1;
    private boolean permissive;
    private String statusDirectory;
    private String importFileName;
    private static final Joiner CSV_JOINER = Joiner.on(",").skipNulls();
    private final String TEMP_DIR_PREFIX = "_temp";

    private static final Logger LOG = Logger.getLogger(SparkDataSetProcessor.class);

    private ExplainNode.SparkExplainKind sparkExplainKind = ExplainNode.SparkExplainKind.NONE;
    private LinkedList<IndentedString> explainStrings = new LinkedList<>();
    private LinkedList<IndentedString> tempOperationStrings = new LinkedList<>();
    private LinkedList<List<IndentedString>> stashedSpliceOperationStrings = new LinkedList<>();
    private LinkedList<List<IndentedString>> spliceOperationStrings = new LinkedList<>();

    private TreeMap<Integer, Integer> numLeadingSpaces = new TreeMap<>();
    private TreeMap<Integer, String>  spacesMap = new TreeMap<>();

    // The depth of the current operation being processed via getDataSet
    // in the operation tree.
    private int opDepth = 0;
    private boolean accumulators;

    public SparkDataSetProcessor() {
        accumulators = EngineDriver.driver().getConfiguration().getSparkAccumulatorsEnabled();
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
        return getEmpty(name, null);
    }

    @Override
    public <V> DataSet<V> getEmpty(String name, OperationContext context) {
        if (context == null)
            return new SparkDataSet<>(SpliceSpark.getContext().parallelize(Collections.<V>emptyList(),1), name);
        try {
            return new NativeSparkDataSet<>(
                    SpliceSpark.getSession().createDataFrame(
                            SpliceSpark.getContext().emptyRDD(),
                            context.getOperation().schema()), context);
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
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
        OperationContext<Op> operationContext =
                accumulators
                        ? new SparkOperationContext<>(spliceOperation, broadcastedActivation)
                        : new SparkLeanOperationContext<>(spliceOperation, broadcastedActivation);
        spliceOperation.setOperationContext(operationContext);
        if (permissive) {
            operationContext.setPermissive(statusDirectory, importFileName, failBadRecordCount);
        }
        return operationContext;
    }

    @SuppressFBWarnings(value = "NP_LOAD_OF_KNOWN_NULL_VALUE",justification = "Intentional")
    @Override
    public <Op extends SpliceOperation> OperationContext<Op> createOperationContext(Activation activation) {
        BroadcastedActivation ba = activation != null ? broadcastedActivation : null;
        return accumulators
                ? new SparkOperationContext<>(activation, ba)
                : new SparkLeanOperationContext<>(activation, ba);
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
            return new SparkDataSet(rdd,OperationContext.Scope.READ_TEXT_FILE.displayName());
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
    public void stopJobGroup(String jobName) {
        SpliceSpark.getContext().cancelJobGroup(jobName);
    }

    private transient BroadcastedActivation broadcastedActivation;

    private void setupBroadcastedActivation(Activation activation, SpliceOperation root){
        if (broadcastedActivation == null) {
            broadcastedActivation = new BroadcastedActivation(activation, root);
        }
    }

    @Override
    public Partitioner getPartitioner(DataSet<ExecRow> dataSet, ExecRow template, int[] keyDecodingMap, boolean[] keyOrder, int[] rightHashKeys) {
        return new HBasePartitioner(dataSet, template, keyDecodingMap, keyOrder, rightHashKeys);
    }

    @Override
    public <V> DataSet<V> readParquetFile(StructType tableSchema, int[] baseColumnMap, int[] partitionColumnMap,
                                          String location, OperationContext context, Qualifier[][] qualifiers,
                                          DataValueDescriptor probeValue,  ExecRow execRow, boolean useSample,
                                          double sampleFraction) throws StandardException {
        try {
            Dataset<Row> table = null;
            ExternalTableUtils.preSortColumns(tableSchema.fields(), partitionColumnMap);

            try {
                table = SpliceSpark.getSession()
                        .read()
                        .schema(tableSchema)
                        .parquet(location);
            } catch (Exception e) {
                return handleExceptionSparkRead(e, location, false);
            }

            checkNumColumns(location, baseColumnMap, table);
            ExternalTableUtils.sortColumns(table.schema().fields(), partitionColumnMap);
            return externalTablesPostProcess(baseColumnMap, context, qualifiers, probeValue,
                    execRow, useSample, sampleFraction, table);
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
        }
    }

    @Override
    public <V> DataSet<V> readAvroFile(StructType tableSchema, int[] baseColumnMap, int[] partitionColumnMap,
                                       String location, OperationContext context, Qualifier[][] qualifiers,
                                       DataValueDescriptor probeValue,  ExecRow execRow,
                                       boolean useSample, double sampleFraction) throws StandardException {
        try {
            Dataset<Row> table = null;
            StructType tableSchemaCopy =
                    new StructType(Arrays.copyOf(tableSchema.fields(), tableSchema.fields().length));

            // Infer schema from external files
            // todo: this is slow on bigger directories, as it's calling getExternalFileSchema,
            // which will do a spark.read() before doing the spark.read() here ...
            StructType dataSchema = ExternalTableUtils.getDataSchema(this, tableSchema, partitionColumnMap, location, "a");
            if(dataSchema == null)
                return getEmpty(RDDName.EMPTY_DATA_SET.displayName(), context);

            try {
                SparkSession spark = SpliceSpark.getSession();
                // Creates a DataFrame from a specified file
                table = spark.read().schema(dataSchema).format("com.databricks.spark.avro").load(location);
            } catch (Exception e) {
                return handleExceptionSparkRead(e, location, false);
            }
            checkNumColumns(location, baseColumnMap, table);
            table = ExternalTableUtils.castDateTypeInAvroDataSet(table, tableSchemaCopy);
            ExternalTableUtils.sortColumns(table.schema().fields(), partitionColumnMap);

            return externalTablesPostProcess(baseColumnMap, context, qualifiers, probeValue,
                    execRow, useSample, sampleFraction, table);
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
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
    private <V> DataSet<V> handleExceptionInferSchema(Exception e, String location) throws Exception {
        // Cannot Infer Schema, Argh
        if ((e instanceof AnalysisException || e instanceof FileNotFoundException) && e.getMessage() != null &&
                (e.getMessage().startsWith("Unable to infer schema") || e.getMessage().startsWith("No Avro files found"))) {
            // Lets check if there are existing files...
           if (ExternalTableUtils.isEmptyDirectory(location)) // Handle Empty Directory
                return getEmpty();
        }
        throw e;
    }

    private <V> DataSet<V> handleExceptionSparkRead(Exception e, String location, boolean checkEmpty) throws Exception {
        // Cannot Infer Schema, Argh
        if( e instanceof org.apache.spark.sql.AnalysisException )
        {
            if( e.getMessage().startsWith("Path does not exist"))
                throw StandardException.newException(SQLState.EXTERNAL_TABLES_LOCATION_NOT_EXIST, location);
            if( checkEmpty && e.getMessage().startsWith("Unable to infer schema") && ExternalTableUtils.isEmptyDirectory(location))
                return getEmpty();

        }
        throw StandardException.newException(SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
    }

    @SuppressFBWarnings(value = "NP_NULL_ON_SOME_PATH",justification = "Intentional")
    @Override
    public StructType getExternalFileSchema(String storedAs, String location, boolean mergeSchema, CsvOptions csvOptions) throws StandardException {
        StructType schema = null;
        Configuration conf = HConfiguration.unwrapDelegate();
        FileSystem fs = null;
        Path temp = null;
        // normalize location string
        location = new Path(location).toString();
        try {

            if (!mergeSchema) {
                fs = FileSystem.get(URI.create(location), conf);
                String fileName = getFile(fs, location);
                boolean canWrite = true;
                if( (fileName == null || (fs.getFileStatus(new Path(location)).getPermission().toShort() & 0222) == 0 )) {
                    canWrite = false;
                }
                else {
                    temp = new Path(location, TEMP_DIR_PREFIX + "_" + UUID.randomUUID().toString().replaceAll("-", ""));

                    try {
                        fs.mkdirs(temp);
                    } catch (IOException e) {
                        canWrite = false;
                        temp = null;
                    }
                }

                if( canWrite )
                {
                    SpliceLogUtils.info(LOG, "created temporary directory %s", temp);
                    // Copy a data file to temp directory
                    int index = fileName.indexOf(location);
                    if (index != -1) {
                        String s = fileName.substring(index + location.length() + 1);
                        Path destDir = new Path(temp, s);
                        try {
                            FileUtil.copy(fs, new Path(fileName), fs, destDir, false, conf);
                            location = temp.toString();
                        } catch (IOException e) {
                            canWrite = false;
                        }
                    }
                }

                if( !canWrite )
                {
                    SpliceLogUtils.info(LOG, "couldn't create temporary directory %s, " +
                            "will read schema from whole directory", temp);
                }
            }
            try {
                Dataset dataset = null;
                String mergeSchemaOption = mergeSchema ? "true" : "false";
                if (storedAs != null) {
                    if (storedAs.toLowerCase().equals("p")) {
                        dataset = SpliceSpark.getSession()
                                .read()
                                .option("mergeSchema", mergeSchemaOption)
                                .parquet(location);
                    } else if (storedAs.toLowerCase().equals("a")) {
                        // spark does not support schema merging for avro
                        dataset = SpliceSpark.getSession()
                                .read()
                                .option("ignoreExtension", false)
                                .format("com.databricks.spark.avro")
                                .load(location);
                    } else if (storedAs.toLowerCase().equals("o")) {
                        // spark does not support schema merging for orc
                        dataset = SpliceSpark.getSession()
                                .read()
                                .orc(location);
                    } else if (storedAs.toLowerCase().equals("t")) {
                        // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
                        dataset = SpliceSpark.getSession().read().options(getCsvOptions(csvOptions)).csv(location);
                    } else {
                        throw new UnsupportedOperationException("Unsupported storedAs " + storedAs);
                    }
                    dataset.printSchema();
                    schema = dataset.schema();
                }
            } catch (Exception e) {
                handleExceptionInferSchema(e, location);
            } finally {
                if (!mergeSchema && fs != null && temp!= null && fs.exists(temp)){
                    fs.delete(temp, true);
                    SpliceLogUtils.info(LOG, "deleted temporary directory %s", temp);
                }
            }
        }catch (Exception e) {
            throw StandardException.newException(SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
        }

        return schema;
    }

    /**
     * Return a data file from external table storage directory
     * @param fs
     * @param location
     * @return
     * @throws IOException
     */
    private String getFile(FileSystem fs, String location) throws IOException {

        Path path = new Path(location);
        String name = path.getName();

        if (!fs.isDirectory(path) && !name.startsWith(".") && !name.equals("_SUCCESS"))
            return location;
        else {
            FileStatus[] fileStatuses = fs.listStatus(path);
            if (!fs.isDirectory(path) || fileStatuses.length == 0)
                return null;
            for (FileStatus fileStatus : fileStatuses) {
                String p = fileStatus.getPath().getName();
                if (!p.startsWith(TEMP_DIR_PREFIX)) {
                    String file = getFile(fs, fileStatus.getPath().toString());
                    if (file != null)
                        return file;
                }
            }
        }
        return  null;
    }



    @Override
    public void createEmptyExternalFile(StructField[] fields, int[] baseColumnMap, int[] partitionBy, String storedAs, String location, String compression) throws StandardException {
        try{
            StructType nschema = ExternalTableUtils.supportAvroDateType(DataTypes.createStructType(fields),storedAs);

            Dataset<Row> empty = SpliceSpark.getSession()
                        .createDataFrame(new ArrayList<Row>(), nschema);


            List<String> partitionByCols = new ArrayList();
            for (int i = 0; i < partitionBy.length; i++) {
                partitionByCols.add(fields[partitionBy[i]].name());
            }
            if (storedAs!=null) {
                if (storedAs.toLowerCase().equals("p")) {
                    compression = SparkDataSet.getParquetCompression(compression);
                    empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                            .mode(SaveMode.Append).parquet(location);
                }
                else if (storedAs.toLowerCase().equals("a")) {
                    compression = SparkDataSet.getAvroCompression(compression);
                    /*
                    empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                            .mode(SaveMode.Append).format("com.databricks.spark.avro").save(location);
                     */
                    empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                            .mode(SaveMode.Append).format("com.databricks.spark.avro").save(location);
                }
                else if (storedAs.toLowerCase().equals("o")) {
                    empty.write().option("compression",compression).partitionBy(partitionByCols.toArray(new String[partitionByCols.size()]))
                            .mode(SaveMode.Append).orc(location);
                }
                else if (storedAs.toLowerCase().equals("t")) {
                    // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
                    empty.write().option("compression",compression).option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss.SSSZZ").mode(SaveMode.Append).csv(location);
                }
            }
        }

        catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
        }
    }


    @Override
    public void dropPinnedTable(long conglomerateId) throws StandardException {
        if (SpliceSpark.getSession().catalog().isCached("SPLICE_"+conglomerateId)) {
            SpliceSpark.getSession().catalog().uncacheTable("SPLICE_"+conglomerateId);
            SpliceSpark.getSession().catalog().dropTempView("SPLICE_"+conglomerateId);

        }
    }

    @Override
    public Boolean isCached(long conglomerateId) throws StandardException {
        return  SpliceSpark.getSession().catalog().tableExists("SPLICE_"+conglomerateId)
                && SpliceSpark.getSession().catalog().isCached("SPLICE_"+conglomerateId);

    }

    private Dataset<Row> processExternalDataset(
                ExecRow execRow,
                Dataset<Row> rawDataset, int[] baseColumnMap, Qualifier[][] qualifiers,
                DataValueDescriptor probeValue) throws StandardException {
        String[] allCols = rawDataset.columns();
        List<Column> cols = new ArrayList();

        for (int i = 0; i < baseColumnMap.length; i++) {
            if (baseColumnMap[i] != -1) {
                Column col = new Column(allCols[i]);
                DataValueDescriptor dvd = execRow.getColumn(baseColumnMap[i] + 1);

                if ( dvd instanceof SQLChar || dvd instanceof SQLVarchar )
                    col = convertSparkStringColToCharVarchar(col, dvd, allCols[i]);

                cols.add(col);
            }
        }

        Dataset dataset = rawDataset
                .select(cols.toArray(new Column[cols.size()]));
        if (qualifiers !=null) {
            Column filter = createFilterCondition(dataset,allCols, qualifiers, baseColumnMap, probeValue);
            if (filter != null) {
                dataset = dataset.filter(filter);
            }
        }
        return dataset;

    }

    private <V> DataSet<V> externalTablesPostProcess(int[] baseColumnMap, OperationContext context,
                                                     Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow,
                                                     boolean useSample, double sampleFraction, Dataset<Row> table) throws StandardException {

        table = processExternalDataset(execRow, table, baseColumnMap, qualifiers, probeValue);

        if (useSample) {
            return new NativeSparkDataSet(table
                    .sample(false, sampleFraction), context);
        } else {
            return new NativeSparkDataSet(table, context);
        }
    }

    /**
     * since spark has only strings, not CHAR/VARCHAR,
     * we need to "create" CHAR/VARCHAR column out of string columns
     * - for CHAR, we need to right-pad strings
     * - for CHAR/VARCHAR, we need to make sure we're considering the maximum string length
     *   note that we will use Java/Scala String length, which is measured in
     *   UTF-16 characters, which is NOT the byte length and also NOT necessarily the character length.
     *   e.g. the single character U+1F602 (https://www.fileformat.info/info/unicode/char/1f602/index.htm)
     *   is encoded as 0xD83D 0xDE02 and therefore has length 2.
     */
    private Column convertSparkStringColToCharVarchar(Column col, DataValueDescriptor dvd, String name) {
        //
        if (dvd instanceof SQLChar &&
                !(dvd instanceof SQLVarchar)) {
            SQLChar sc = (SQLChar) dvd;
            if (sc.getSqlCharSize() > 0) {
                Column adapted = functions.rpad(col, sc.getSqlCharSize(), " ");
                col = adapted.as(name);
            }
        }
        else if ( dvd instanceof SQLChar) {
            // rpad will already cut strings
            SQLChar sc = (SQLChar) dvd;
            if (sc.getSqlCharSize() > 0) {
                Column adapted = functions.substring(col, 0, sc.getSqlCharSize() );
                col = adapted.as(name);
            }
        }
        return col;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet<V> readPinnedTable(
            long conglomerateId, int[] baseColumnMap, String location, OperationContext context,
            Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow) throws StandardException {
        try {
            Dataset<Row> table = SpliceSpark.getSession().table("SPLICE_"+conglomerateId);
            table = processExternalDataset(execRow, table,baseColumnMap,qualifiers,probeValue);
            return new SparkDataSet(table
                    .rdd().toJavaRDD()
                    .map(new RowToLocatedRowFunction(context,execRow)));
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.PIN_READ_FAILURE, e, e.getMessage());
        }
    }
    private <V> DataSet<V> checkExistingOrEmpty( String location, OperationContext context ) throws StandardException, IOException {
        FileInfo fileinfo = ImportUtils.getImportFileInfo(location);
        if( !fileinfo.exists() )
            throw StandardException.newException(SQLState.EXTERNAL_TABLES_LOCATION_NOT_EXIST, location);
        if ( fileinfo.isEmptyDirectory() ) // Handle Empty Directory
            return getEmpty(RDDName.EMPTY_DATA_SET.displayName(), context);
        else
            return null;
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <V> DataSet<V> readORCFile(int[] baseColumnMap,int[] partitionColumnMap, String location,
                                          OperationContext context, Qualifier[][] qualifiers,
                                      DataValueDescriptor probeValue, ExecRow execRow,
                                      boolean useSample, double sampleFraction, boolean statsjob) throws StandardException {
        assert baseColumnMap != null:"baseColumnMap Null";
        assert partitionColumnMap != null:"partitionColumnMap Null";
        try {
            DataSet<V> empty_ds = checkExistingOrEmpty( location, context );
            if( empty_ds != null ) return empty_ds;

            SpliceORCPredicate predicate = new SpliceORCPredicate(qualifiers,baseColumnMap,execRow.createStructType(baseColumnMap));
            Configuration configuration = new Configuration(HConfiguration.unwrapDelegate());
            configuration.set(SpliceOrcNewInputFormat.SPLICE_PREDICATE,predicate.serialize());
            configuration.set(SpliceOrcNewInputFormat.SPARK_STRUCT,execRow.createStructType(baseColumnMap).json());
            configuration.set(SpliceOrcNewInputFormat.SPLICE_COLUMNS,intArrayToString(baseColumnMap));
            configuration.set(SpliceOrcNewInputFormat.SPLICE_PARTITIONS,intArrayToString(partitionColumnMap));
            if (statsjob)
                configuration.set(SpliceOrcNewInputFormat.SPLICE_COLLECTSTATS, "true");

            JavaRDD<Row> rows;
            try {
                rows = SpliceSpark.getContext().newAPIHadoopFile(
                        location,
                        SpliceOrcNewInputFormat.class,
                        NullWritable.class,
                        Row.class,
                        configuration)
                        .values();
            }
            catch (Exception e) {
                return handleExceptionSparkRead(e, location, false);
            }

            if (useSample) {
                return new SparkDataSet(rows.sample(false,sampleFraction).map(new RowToLocatedRowFunction(context, execRow)));
            } else {
                return new SparkDataSet(rows.map(new RowToLocatedRowFunction(context, execRow)));
            }
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
        }
    }

    private String intArrayToString(int[] ints) {
        StringBuilder sb = new StringBuilder();
        boolean first = true;
        for (int i = 0 ; i < ints.length; i++) {
            if (!first)
                sb.append(",");
            sb.append(ints[i]);
            first = false;
        }
        return sb.toString();
    }

    @Override
    public <V> DataSet<ExecRow> readTextFile(SpliceOperation op, String location, CsvOptions csvOptions, int[] baseColumnMap,
                                      OperationContext context, Qualifier[][] qualifiers, DataValueDescriptor probeValue, ExecRow execRow,
                                                boolean useSample, double sampleFraction) throws StandardException {
        assert baseColumnMap != null:"baseColumnMap Null";
        try {
            Dataset<Row> table = null;
            try {
                table = SpliceSpark.getSession().read().options(getCsvOptions(csvOptions)).csv(location);
                if (table.schema().fields().length == 0)
                    return getEmpty();
            } catch (Exception e) {
                return handleExceptionSparkRead(e, location, true);
            }

            checkNumColumns(location, baseColumnMap, table);
            if (op == null) {
                // stats collection scan
                for(int index = 0; index < execRow.schema().fields().length; index++) {
                    StructField ft = table.schema().fields()[index];
                    Column cl = new Column(ft.name()).cast(execRow.schema().fields()[index].dataType());
                    table = table.withColumn(ft.name(), cl);
                }
            } else {
                for( int index = 0; index< baseColumnMap.length; index++) {
                    if (baseColumnMap[index] != -1) {
                        StructField ft = table.schema().fields()[index];
                        Column cl = new Column(ft.name()).cast(execRow.schema().fields()[baseColumnMap[index]].dataType());
                        table = table.withColumn(ft.name(), cl);
                    }
                }
            }


            return externalTablesPostProcess(baseColumnMap, context, qualifiers, probeValue,
                    execRow, useSample, sampleFraction, table);
        } catch (Exception e) {
            throw StandardException.newException(
                    SQLState.EXTERNAL_TABLES_READ_FAILURE, e, e.getMessage());
        }
    }

    static String unescape(String type, String in) throws StandardException {
        try{
            return ImportUtils.unescape(in);
        }
        catch( IOException e)
        {
            throw StandardException.newException(SQLState.LANG_FORMAT_EXCEPTION, e, type + ". " + e.getMessage());
        }
    }

    /**
     * @param csvOptions
     * @return spark dataframereader options, see
     *         https://spark.apache.org/docs/latest/api/java/org/apache/spark/sql/DataFrameReader.html#csv-scala.collection.Seq-
     * @throws IOException
     */
    public static HashMap<String, String> getCsvOptions(CsvOptions csvOptions) throws StandardException {
        HashMap<String, String> options = new HashMap<String, String>();

        // spark-2.2.0: commons-lang3-3.3.2 does not support 'XXX' timezone, specify 'ZZ' instead
        String timestampFormat = "yyyy-MM-dd'T'HH:mm:ss.SSSZZ";
        options.put("timestampFormat", timestampFormat);

        String delimited = unescape("TERMINATED BY", csvOptions.columnDelimiter);
        String escaped = unescape( "ESCAPED BY", csvOptions.escapeCharacter);
        String lines = unescape( "LINES SEPARATED BY", csvOptions.lineTerminator);

        if (delimited != null) // default ,
            options.put("sep", delimited);
        if (escaped != null)
            options.put("escape", escaped); // default \
        if( lines != null ) // default \n
            options.put("lineSep", lines);
        return options;
    }

    /// check that we don't access a column that's not there with baseColumnMap
    private static void checkNumColumns(String location, int[] baseColumnMap, Dataset<Row> table) throws StandardException {
        if( baseColumnMap.length > table.schema().fields().length) {
            throw StandardException.newException(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE,
                    baseColumnMap.length, table.schema().fields().length,
                    location, ExternalTableUtils.getSuggestedSchema(table.schema()));
        }
    }

    private static Column createFilterCondition(Dataset dataset,String[] allColIdInSpark, Qualifier[][] qual_list,
                                                int[] baseColumnMap, DataValueDescriptor probeValue) throws StandardException {
        assert qual_list!=null:"qualifier[][] passed in is null";
        boolean     row_qualifies = true;
        Column andCols = null;
        for (int i = 0; i < qual_list[0].length; i++) {
            Qualifier q = qual_list[0][i];
            if (q.getVariantType() == Qualifier.VARIANT)
                continue; // Cannot Push Down Qualifier
            Column col = dataset.col(allColIdInSpark[q.getStoragePosition()]);
            q.clearOrderableCache();

            DataValueDescriptor dvd = probeValue;
            if (dvd == null || i != 0) {
                dvd = q.getOrderable();
            }

            Object value = dvd.getObject();
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
                default:
                    throw new UnsupportedOperationException("Unknown operator: " + q.getOperator());
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

                Column orCol = dataset.col(allColIdInSpark[q.getStoragePosition()]);

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
                    default:
                        throw new UnsupportedOperationException("Unknown operator: " + q.getOperator());
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

    @Override
    public TableChecker getTableChecker(String schemaName, String tableName, DataSet table,
                                        KeyHashDecoder tableKeyDecoder, ExecRow tableKey, TxnView txn, boolean fix,
                                        int[] baseColumnMap, boolean isSystemTable) {
        return new SparkTableChecker(schemaName, tableName, table, tableKeyDecoder, tableKey, txn, fix, baseColumnMap,
                isSystemTable);
    }

    @Override
    public boolean isSparkExplain() { return sparkExplainKind != ExplainNode.SparkExplainKind.NONE; }

    @Override
    public ExplainNode.SparkExplainKind getSparkExplainKind() { return sparkExplainKind; }

    @Override
    public void setSparkExplain(ExplainNode.SparkExplainKind newValue) { sparkExplainKind = newValue; }

    private void prependIndentedStrings(List<IndentedString> indentedStrings) {
        tempOperationStrings.addAll(0, indentedStrings);
    }

    @Override
    public void prependSpliceExplainString(String explainString) {
        StringBuilder sb = new StringBuilder();
        if (!explainString.isEmpty())
            sb.append("-> ");
        // Strip out newlines and trailing spaces.
        sb.append(explainString.replace("\n","").replaceFirst("\\s++$", ""));
        tempOperationStrings.addFirst(new IndentedString(getOpDepth(), sb.toString()));
    }

    @Override
    public void appendSpliceExplainString(String explainString) {
        StringBuilder sb = new StringBuilder();
        if (!explainString.isEmpty())
            sb.append("-> ");
        // Strip out newlines and trailing spaces.
        sb.append(explainString.replace("\n","").replaceFirst("\\s++$", ""));
        tempOperationStrings.addLast(new IndentedString(getOpDepth(), sb.toString()));
    }

    @Override
    public void finalizeTempOperationStrings() {
        if (!tempOperationStrings.isEmpty()) {
            spliceOperationStrings.addFirst(tempOperationStrings);
            tempOperationStrings = new LinkedList<>();
        }
    }

    private void stashTempOperationStrings() {
        if (!tempOperationStrings.isEmpty()) {
            stashedSpliceOperationStrings.addFirst(tempOperationStrings);
            tempOperationStrings = new LinkedList<>();
        }
    }

    private void popStashedOperationStrings() {
        if (!stashedSpliceOperationStrings.isEmpty()) {
            spliceOperationStrings.addAll(0, stashedSpliceOperationStrings);
            stashedSpliceOperationStrings = new LinkedList<>();
        }
    }

    private boolean topOfSavedOperationsIsSibling(IndentedString newSparkExplain) {
        if (!spliceOperationStrings.isEmpty() &&
             spliceOperationStrings.get(0).get(0).getIndentationLevel() == newSparkExplain.getIndentationLevel())
            return true;
        return false;
    }

    public void popSpliceOperation() {
        if (!spliceOperationStrings.isEmpty() &&
             spliceOperationStrings.get(0).get(0).getIndentationLevel() == opDepth+1)
            tempOperationStrings.addAll(0, spliceOperationStrings.remove(0));
    }

    @Override
    public void prependSparkExplainStrings(List<String> stringsToAdd, boolean firstOperationSource, boolean lastOperationSource) {
        if (firstOperationSource)
            finalizeTempOperationStrings();
        IndentedString newSparkExplain =
            new IndentedString(getOpDepth()+1, stringsToAdd);
        newSparkExplain = fixupSparkExplain(newSparkExplain);
        boolean isSecondSourceOfJoin = !firstOperationSource && lastOperationSource;
        boolean siblingPresent = isSecondSourceOfJoin &&
                                 tempOperationStrings.size() >= 1 &&
                                 tempOperationStrings.get(0).getIndentationLevel() ==
                                 newSparkExplain.getIndentationLevel();

        // Handle join cases where there is still one element left in
        // tempOperationStrings at the same of higher level as the spark operation,
        // which needs to get tied together via a join.
        if (tempOperationStrings.isEmpty() ||
            tempOperationStrings.size() >= 1 &&
            tempOperationStrings.get(0).getIndentationLevel() <=
            newSparkExplain.getIndentationLevel()) {
            if (!siblingPresent) {
                finalizeTempOperationStrings();
                if (isSecondSourceOfJoin &&
                    topOfSavedOperationsIsSibling(newSparkExplain))
                    popSpliceOperation();
            }
            tempOperationStrings.add(new IndentedString(getOpDepth() + 1, "-> NativeSparkDataSet"));
            newSparkExplain.setIndentationLevel(getOpDepth() + 2);
            tempOperationStrings.add(newSparkExplain);
        }
        else
            explainStrings.addFirst(newSparkExplain);
    }

    private int findIndentation(Map<Integer, Integer>  numLeadingSpaces,
                                int indentationLevel,
                                boolean doLookup) {
        Integer numSpaces = null;
        if (doLookup)
            numSpaces = numLeadingSpaces.get(indentationLevel);

        if (numSpaces == null && doLookup) {
            for (int i = indentationLevel; i >= 0; i--) {
                Integer foundSpaces = numLeadingSpaces.get(i);
                if (foundSpaces == null)
                    continue;
                numSpaces = foundSpaces + (indentationLevel - i) * 2;
                numLeadingSpaces.put(indentationLevel, numSpaces);
                break;
            }
        }
        if (numSpaces == null)
            numSpaces = indentationLevel * 2;

        return numSpaces;
    }

    private IndentedString
    fixupSparkExplain(IndentedString sparkExplain) {
        finalizeTempOperationStrings();
        if (spliceOperationStrings.isEmpty())
            return sparkExplain;

        IndentedString newSparkExplain = null;
        LinkedList<String> newTextLines = new LinkedList<>();
        for (String string:sparkExplain.getTextLines()) {
            newTextLines.add(string);
            int matchIndex =
               StringUtils.indexOfAny(string, new String[]{"Scan ExistingRDD", "FileScan", "LogicalRDD", "Relation[", "ReusedExchange"});

            int matchReusedExchange =
               StringUtils.indexOfAny(string, new String[]{"ReusedExchange"});

            if (!spliceOperationStrings.isEmpty() && matchIndex != -1) {
                List<IndentedString> list = spliceOperationStrings.removeLast();
                int baseIndentationLevel = list.get(0).getIndentationLevel();
                int leadingSpaces = matchIndex;
                {
                    // Native spark explain is indented 3 spaces in getNativeSparkExplain().
                    int numSpaces = leadingSpaces + 3;

                    char[] charArray = new char[numSpaces];
                    Arrays.fill(charArray, ' ');
                    String prependString = new String(charArray);
                    // Adjust the global indentation setting in cases we are dealing
                    // with one source of a join, and the other source will
                    // be processed later.
                    numLeadingSpaces.put(baseIndentationLevel, 1);
                    numLeadingSpaces.put(baseIndentationLevel+1, prependString.length() + sparkExplain.getIndentationLevel()*2);
                    spacesMap.put(baseIndentationLevel, prependString);
                    for (Integer i = numLeadingSpaces.higherKey(baseIndentationLevel); i != null;
                                 i = numLeadingSpaces.higherKey(i))
                        numLeadingSpaces.remove(i);
                    for (Integer i = spacesMap.higherKey(baseIndentationLevel); i != null;
                                 i = spacesMap.higherKey(i))
                        spacesMap.remove(i);

                }
                boolean notMatched;
                do {
                    ListIterator<IndentedString> iter = list.listIterator();
                    notMatched = true;

                    IndentedString firstStringInOperationSet = list.get(0);

                    if (firstStringInOperationSet != null &&
                        firstStringInOperationSet.getIndentationLevel() <= getOpDepth() + 1) {

                        stashTempOperationStrings();
                        prependIndentedStrings(list);
                    }
                    else
                    while (iter.hasNext()) {
                        IndentedString istr = iter.next();
                        notMatched = false;
                        if (matchReusedExchange == -1)
                        for (String str : istr.getTextLines()) {
                            int indentationLevel = istr.getIndentationLevel() - baseIndentationLevel;
                            int numExtraSpaces = indentationLevel * 2;
                            int numSpaces = leadingSpaces + numExtraSpaces;
                            char[] charArray = new char[numSpaces];
                            Arrays.fill(charArray, ' ');
                            String prependString = new String(charArray);
                            newTextLines.add(prependString + str);
                        }
                    }
                    if (notMatched && !spliceOperationStrings.isEmpty()) {
                        list = spliceOperationStrings.removeLast();
                        baseIndentationLevel = list.get(0).getIndentationLevel();
                    }
                    else
                        notMatched = false;
                } while (notMatched);
            }
        }
        spacesMap.clear();
        stashTempOperationStrings();
        popStashedOperationStrings();
        newSparkExplain = new IndentedString(sparkExplain.getIndentationLevel(), newTextLines);
        return newSparkExplain;
    }

    @Override
    public List<String> getNativeSparkExplain() {
        int indentationLevel = 0;

        finalizeTempOperationStrings();
        for (List<IndentedString> indentedStrings:spliceOperationStrings)
            explainStrings.addAll(0, indentedStrings);  

        if (!explainStrings.isEmpty())
            indentationLevel = explainStrings.getFirst().getIndentationLevel();

        numLeadingSpaces.put(indentationLevel, 0);
        spacesMap.put(indentationLevel, "");

        int previousIndentationLevel = -1;
        int maxIndentationLevel = -1;
        List<String> sparkExplain = new LinkedList<>();
        boolean firstLine = true;
        for (IndentedString strings:explainStrings) {
            if (strings.getIndentationLevel() <= previousIndentationLevel) {
                for (int i = maxIndentationLevel; i >= strings.getIndentationLevel(); i--) {
                    spacesMap.remove(i);
                    numLeadingSpaces.remove(i);
                }
            }
            String prependString = spacesMap.get(strings.getIndentationLevel());
            boolean nativeSpark = strings.getTextLines().size() > 1;
            if (prependString == null)
            {
                int indentation = findIndentation(numLeadingSpaces,
                                                  strings.getIndentationLevel(),
                                                  nativeSpark);
                char[] charArray = new char[indentation];
                Arrays.fill(charArray, ' ');
                prependString = new String(charArray);
                spacesMap.put(strings.getIndentationLevel(), prependString);
            }
            if (nativeSpark) {
                if (firstLine)
                    sparkExplain.add(prependString + "NativeSparkDataSet");
                else if (!sparkExplain.get(sparkExplain.size()-1).contains("NativeSparkDataSet"))
                    sparkExplain.add(prependString + "-> NativeSparkDataSet");
                prependString = prependString + "   ";
            }
            int newIndentPos = prependString.length();
            for (String s:strings.getTextLines()) {
                String newString = prependString + s;
                sparkExplain.add(newString);
                int tempIndentPos = newString.indexOf("+-");
                if (tempIndentPos > newIndentPos) {
                    newIndentPos = tempIndentPos;
                    numLeadingSpaces.put(strings.getIndentationLevel(), newIndentPos);
                }
            }
            previousIndentationLevel = strings.getIndentationLevel();
            if (previousIndentationLevel > maxIndentationLevel)
                maxIndentationLevel = previousIndentationLevel;
            firstLine = false;
        }
        return sparkExplain;
    }

    @Override
    public int getOpDepth() { return opDepth; }

    @Override
    public void incrementOpDepth() {
        if (isSparkExplain())
            opDepth++;
    }

    @Override
    public void decrementOpDepth() {
        if (isSparkExplain())
            opDepth--;
    }

    @Override
    public void resetOpDepth() { opDepth = 0; }

    public <V> DataSet<ExecRow> readKafkaTopic(String topicName, OperationContext context) throws StandardException {
        Properties props = new Properties();
        String consumerGroupId = "spark-consumer-dss-sdsp";
        String bootstrapServers = SIDriver.driver().getConfiguration().getKafkaBootstrapServers();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, consumerGroupId+"-"+UUID.randomUUID());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ExternalizableDeserializer.class.getName());

        KafkaConsumer<Integer, Externalizable> consumer = new KafkaConsumer<Integer, Externalizable>(props);
        List ps = consumer.partitionsFor(topicName);
        List<Integer> partitions = new ArrayList<>(ps.size());
        for (int i = 0; i < ps.size(); ++i) {
            partitions.add(i);
        }
        consumer.close();

        SparkDataSet rdd = new SparkDataSet(SpliceSpark.getContext().parallelize(partitions, partitions.size()));
        return rdd.flatMap(new KafkaReadFunction(context, topicName, bootstrapServers));
    }
}
