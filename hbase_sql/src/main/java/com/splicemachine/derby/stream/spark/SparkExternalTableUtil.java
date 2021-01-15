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
 *
 */

package com.splicemachine.derby.stream.spark;

import com.splicemachine.access.api.DistributedFileSystem;
import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.derby.stream.utils.ExternalTableUtils;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.spark.splicemachine.PartitionSpec;
import com.splicemachine.spark.splicemachine.SplicePartitioningUtils;
import com.splicemachine.system.CsvOptions;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.*;
import scala.Option;
import scala.collection.JavaConverters;

import java.io.IOException;
import java.net.URISyntaxException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Supplier;

import static java.util.stream.Collectors.toList;

public class SparkExternalTableUtil {
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

    /**
     * example:
     * schema = col0, col1, col2, col3, col4, col5, col6
     * partitionColumnMap = 0, 4
     * schema without partitions: col1, col2, col3, col5, col6
     * append partitions to end: col0, col4
     * result: col1, col2, col3, col5, col6, col0, col4
     */
    public static void preSortColumns(StructField[] schema, int[] partitionColumnMap) {
        if (partitionColumnMap.length > 0) {
            // get the partitioned columns and map them to their correct indexes
            HashMap<Integer, StructField> partitions = new HashMap<>();

            // sort the partitioned columns back into their correct respective indexes in schema
            StructField[] schemaCopy = schema.clone();
            for (int i = 0; i < partitionColumnMap.length; ++i) {
                partitions.put(partitionColumnMap[i], schemaCopy[partitionColumnMap[i]]);
            }

            int schemaIndex = 0;
            for (int i = 0; i < schemaCopy.length; i++) {
                if (partitions.containsKey(i)) {
                    continue;
                } else {
                    schema[schemaIndex++] = schemaCopy[i];
                }
            }
            for (int i = 0; i < partitionColumnMap.length; ++i) {
                schema[schemaIndex++] = partitions.get(partitionColumnMap[i]);
            }
        }
    }

    /**
     * this is reversing the sorting done in preSortColumns
     */
    public static void sortColumns(StructField[] schema, int[] partitionColumnMap) {
        if (partitionColumnMap.length > 0) {
            // get the partitioned columns and map them to their correct indexes
            HashMap<Integer, StructField> partitions = new HashMap<>();
            int schemaColumnIndex = schema.length - 1;
            for (int i = partitionColumnMap.length - 1; i >= 0; i--) {
                partitions.put(partitionColumnMap[i], schema[schemaColumnIndex]);
                schemaColumnIndex--;
            }

            // sort the partitioned columns back into their correct respective indexes in schema
            StructField[] schemaCopy = schema.clone();
            int schemaCopyIndex = 0;
            for (int i = 0; i < schema.length; i++) {
                if (partitions.containsKey(i)) {
                    schema[i] = partitions.get(i);
                } else {
                    schema[i] = schemaCopy[schemaCopyIndex++];
                }
            }
        }
    }

    public static boolean isEmptyDirectory(String location) throws Exception {
        DistributedFileSystem dfs = ImportUtils.getFileSystem(location);
        return dfs.getInfo(location).isEmptyDirectory();
    }

    // todo(martinrupp): docu
    public static PartitionSpec parsePartitionsFromFiles(List<Path> files,
                                                         boolean typeInference,
                                                         Set<Path> basePaths,
                                                         StructType userSpecifiedDataTypes,
                                                         TimeZone timeZone) throws IllegalArgumentException {
        List<Path> directories = getDistinctSubdirectoriesOf(files, basePaths);
        return parsePartitions( directories, typeInference, basePaths, userSpecifiedDataTypes, timeZone);
    }

    // todo(martinrupp): docu
    public static List<Path> getDistinctSubdirectoriesOf(List<Path> files, Set<Path> basePaths)
    {
        return files.stream()
                .map( s -> s.getParent() ).distinct()
                .filter( s -> !basePaths.contains(s) ).collect(toList());
    }

    public static PartitionSpec parsePartitions(List<Path> directories,
                                                boolean typeInference,
                                                Set<Path> basePaths,
                                                StructType userDefTypeStruct,
                                                TimeZone timeZone) throws IllegalArgumentException {

        scala.collection.Seq<Path> scala_directories =
                scala.collection.JavaConverters.collectionAsScalaIterableConverter(directories).asScala().toList();
        scala.collection.immutable.Set<Path> scala_basePaths =
                JavaConverters.collectionAsScalaIterableConverter(basePaths).asScala().toSet();

        Option<StructType> scala_userSpecifiedDataTypes = Option.apply(userDefTypeStruct);

        if (timeZone == null) timeZone = TimeZone.getDefault();
        DateFormat dateFormat = new SimpleDateFormat();
        dateFormat.setTimeZone(timeZone);
        boolean caseSensitive = true;
        return SplicePartitioningUtils.parsePartitions(scala_directories, typeInference,
                scala_basePaths, scala_userSpecifiedDataTypes, caseSensitive, dateFormat, dateFormat);
    }

    public static Dataset<Row> castDateTypeInAvroDataSet(Dataset<Row> dataset, StructType tableSchema) {
        int i = 0;
        for (StructField sf : tableSchema.fields()) {
            if (sf.dataType().sameType(DataTypes.DateType)) {
                String colName = dataset.schema().fields()[i].name();
                dataset = dataset.withColumn(colName, dataset.col(colName).cast(DataTypes.DateType));
            }
            i++;
        }
        return dataset;
    }

    /// returns a suggested schema for this schema, e.g. `CREATE EXTERNAL TABLE T (a_float REAL, a_double DOUBLE);`
    public static String getSuggestedSchema(StructType externalSchema, FileInfo fileInfo) {
        StructType partition_schema = null;
        if( fileInfo != null ) {
            FileInfo[] fileInfos = fileInfo.listFilesRecursive();
            List<Path> files = Arrays.stream(fileInfos).map(s -> new Path(s.fullPath())).collect(toList());
            Set<Path> basePaths = Collections.singleton(new Path(fileInfo.fullPath()));
            PartitionSpec partitionSpec = parsePartitionsFromFiles(files, true, basePaths,
                    null, null);
            partition_schema = partitionSpec.partitionColumns();
        }
        StringBuilder sb = new StringBuilder();
        ExternalTableUtils.getSuggestedSchema(sb, externalSchema, partition_schema, "");
        sb.append( ";" );
        if( fileInfo == null ) {
            sb.append(" (note: could not check path, so no PARTITIONED BY information available)");
        }
        return sb.toString();
    }

    static String getSuggestedSchema(StructType externalSchema, String location) {
        try {
            DistributedFileSystem fileSystem = SIDriver.driver().getFileSystem(location);
            return getSuggestedSchema(externalSchema, fileSystem.getInfo(location));
        } catch (IOException | URISyntaxException e) {
            return getSuggestedSchema(externalSchema, (FileInfo) null);
        }
    }

    static String getParquetCompression(String compression )
    {
        // parquet in spark supports: lz4, gzip, lzo, snappy, none, zstd.
        if( compression.equals("zlib") )
            compression = "gzip";
        return compression;
    }

    public static String getAvroCompression(String compression) {
        // avro supports uncompressed, snappy, deflate, bzip2 and xz
        if (compression.equals("none"))
            compression = "uncompressed";
        else if( compression.equals("zlib"))
            compression = "deflate";
        return compression;
    }

    public static void checkSchemaAvro(StructType tableSchema,
                                       StructType externalSchema,
                                       int[] partitionColumnMap,
                                       String location) throws StandardException {

        // only access filesystem if necessary
        Supplier<FileInfo> getFile = () -> {
            try {
                DistributedFileSystem fileSystem = SIDriver.driver().getFileSystem(location);
                return fileSystem.getInfo(location);
            } catch (IOException | URISyntaxException e) {
                return null;
            }
        };

        StructField[] tableFields = tableSchema.fields();
        StructField[] externalFields = externalSchema.fields();

        if (tableFields.length != externalFields.length) {
            throw StandardException.newException(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE,
                    tableFields.length, externalFields.length, location,
                    getSuggestedSchema(externalSchema, getFile.get()) );
        }

        StructField[] partitionedTableFields = new StructField[tableSchema.fields().length];
        Set<Integer> partitionColumns = new HashSet<>();
        for (int pos : partitionColumnMap) {
            partitionColumns.add(pos);
        }
        int index = 0;
        for (int i = 0; i < tableFields.length; ++i) {
            if (!partitionColumns.contains(i)) {
                partitionedTableFields[index++] = tableFields[i];
            }
        }

        for (int i = 0; i < tableFields.length - partitionColumnMap.length; ++i) {

            String tableFiledTypeName = partitionedTableFields[i].dataType().typeName();
            String dataFieldTypeName = externalFields[i].dataType().typeName();
            if (!tableFiledTypeName.equals(dataFieldTypeName)){
                throw StandardException.newException(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES,
                        tableFields[i].name(), ExternalTableUtils.getSqlTypeName(tableFields[i].dataType()),
                        externalFields[i].name(), ExternalTableUtils.getSqlTypeName(externalFields[i].dataType()),
                        location, getSuggestedSchema(externalSchema, getFile.get()) );
            }
        }
    }

    public static void setPartitionColumnTypesAvro(StructType dataSchema, int[] baseColumnMap, StructType tableSchema){

        int ncolumns = dataSchema.fields().length;
        int nPartitions = baseColumnMap.length;
        for (int i = 0; i < baseColumnMap.length; ++i) {
            String name = dataSchema.fields()[ncolumns - i - 1].name();
            org.apache.spark.sql.types.DataType type = tableSchema.fields()[baseColumnMap[nPartitions - i - 1]].dataType();
            boolean nullable = tableSchema.fields()[baseColumnMap[nPartitions - i - 1]].nullable();
            Metadata metadata = tableSchema.fields()[baseColumnMap[nPartitions - i - 1]].metadata();
            StructField field = new StructField(name, type, nullable, metadata);
            dataSchema.fields()[ncolumns - i - 1] = field;
        }
    }

    private static StructType getDataSchemaAvro(DataSetProcessor dsp, StructType tableSchema, int[] partitionColumnMap,
                                                String location, boolean mergeSchema) throws StandardException {
        String storeAs = "a";
        StructType dataSchema =dsp.getExternalFileSchema(storeAs, location, mergeSchema, null,
                null, null, null).getFullSchema();
        tableSchema =  supportAvroDateType(tableSchema, storeAs);
        if (dataSchema != null) {
            SparkExternalTableUtil.checkSchemaAvro(tableSchema, dataSchema, partitionColumnMap, location);

            // set partition column datatype, because the inferred type is not always correct
            setPartitionColumnTypesAvro(dataSchema, partitionColumnMap, tableSchema);
        }
        return dataSchema;
    }

    public static StructType getDataSchemaAvro(DataSetProcessor dsp, StructType tableSchema, int[] partitionColumnMap,
                                               String location) throws StandardException {
        // Infer schema from external files\
        StructType dataSchema = null;
        try {
            dataSchema = getDataSchemaAvro(dsp, tableSchema, partitionColumnMap, location, false);
        }
        catch (StandardException e) {
            String sqlState = e.getSqlState();
            if (sqlState.equals(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE) ||
                    sqlState.equals(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES)) {
                dataSchema = getDataSchemaAvro(dsp, tableSchema, partitionColumnMap, location, true);
            }
            else {
                throw e;
            }
        }
        return dataSchema;
    }

    /**
     * check for Avro date type conversion. Databricks' spark-avro support does not handle date.
     */
    public static StructType supportAvroDateType(StructType schema, String storedAs) {
        if (storedAs.toLowerCase().equals("a")) {
            for (int i = 0; i < schema.size(); i++) {
                StructField column = schema.fields()[i];
                if (column.dataType().equals(DataTypes.DateType)) {
                    StructField replace = DataTypes.createStructField(column.name(), DataTypes.StringType, column.nullable(), column.metadata());
                    schema.fields()[i] = replace;
                }
            }
        }
        return schema;
    }
}
