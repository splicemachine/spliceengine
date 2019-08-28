/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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


package com.splicemachine.derby.stream.utils;

import com.splicemachine.access.api.FileInfo;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.load.ImportUtils;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.derby.stream.iapi.DataSetProcessor;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Created by tgildersleeve on 8/2/17.
 */
public class ExternalTableUtils {

    /*
     check for Avro date type conversion. Databricks' spark-avro support does not handle date.
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

    public static void supportAvroDateTypeColumns(ExecRow execRow) throws StandardException {
        for(int i=0; i < execRow.size(); i++){
            if (execRow.getColumn(i + 1).getTypeName().equals("DATE")) {
                execRow.setColumn(i + 1, new SQLVarchar());
            }
        }
    }


    public static StructType getSchema(Activation activation, long conglomerateId) throws StandardException {

        boolean prepared = false;
        SpliceTransactionResourceImpl transactionResource = null;
        Txn txn = null;
        LanguageConnectionContext lcc = null;
        try {
            if (activation == null) {
                txn = SIDriver.driver().lifecycleManager()
                        .beginTransaction();
                transactionResource = new SpliceTransactionResourceImpl();
                prepared=transactionResource.marshallTransaction(txn);
                lcc = transactionResource.getLcc();
            }
            else {
                lcc = activation.getLanguageConnectionContext();
            }
            DataDictionary dd = lcc.getDataDictionary();
            ConglomerateDescriptor cd = dd.getConglomerateDescriptor(conglomerateId);
            UUID tableId = cd.getTableID();
            TableDescriptor td = dd.getTableDescriptor(tableId);
            ColumnDescriptorList cdl = td.getColumnDescriptorList();
            ExecRow execRow = new ValueRow(cdl.size());
            DataValueDescriptor[] dvds = execRow.getRowArray();
            for (int i = 0; i < cdl.size(); ++i) {
                ColumnDescriptor columnDescriptor = cdl.get(i);
                DataTypeDescriptor dtd = columnDescriptor.getType();
                dvds[i] = dtd.getNull();
            }
            StructType schema = execRow.schema();
            return  schema;
        }
        catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
        finally {
            if(prepared)
                transactionResource.close();
            try {
                if (txn != null)
                    txn.commit();
            } catch (IOException ioe) {
                throw StandardException.plainWrapException(ioe);
            }
        }
    }

    public static void checkSchema(StructType tableSchema,
                                   StructType dataSchema,
                                   int[] partitionColumnMap,
                                   String location) throws StandardException{


        StructField[] tableFields = tableSchema.fields();
        StructField[] dataFields = dataSchema.fields();

        if (tableFields.length != dataFields.length) {
            throw StandardException.newException(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE,
                    tableFields.length, dataFields.length, location);
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
            String dataFieldTypeName = dataFields[i].dataType().typeName();
            if (!tableFiledTypeName.equals(dataFieldTypeName)){
                throw StandardException.newException(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES,
                        tableFields[i].name(),
                        tableFields[i].dataType().toString(),
                        dataFields[i].name(),
                        dataFields[i].dataType().toString(),location);
            }
        }
    }

    private static StructType getDataSchema(DataSetProcessor dsp, StructType tableSchema, int[] partitionColumnMap,
                                     String location, String storeAs, boolean mergeSchema) throws StandardException {
        StructType dataSchema =dsp.getExternalFileSchema(storeAs, location, mergeSchema);
        tableSchema =  ExternalTableUtils.supportAvroDateType(tableSchema, storeAs);
        if (dataSchema != null) {
            ExternalTableUtils.checkSchema(tableSchema, dataSchema, partitionColumnMap, location);

            // set partition column datatype, because the inferred type is not always correct
            setPartitionColumnTypes(dataSchema, partitionColumnMap, tableSchema);
        }
        return dataSchema;
    }

    public static StructType getDataSchema(DataSetProcessor dsp, StructType tableSchema, int[] partitionColumnMap,
                                           String location, String storeAs) throws StandardException {
        // Infer schema from external files\
        StructType dataSchema = null;
        try {
            dataSchema = getDataSchema(dsp, tableSchema, partitionColumnMap, location, storeAs, false);
        }
        catch (StandardException e) {
            String sqlState = e.getSqlState();
            if (sqlState.equals(SQLState.INCONSISTENT_NUMBER_OF_ATTRIBUTE) ||
                    sqlState.equals(SQLState.INCONSISTENT_DATATYPE_ATTRIBUTES)) {
                dataSchema = getDataSchema(dsp, tableSchema, partitionColumnMap, location, storeAs, true);
            }
            else {
                throw e;
            }
        }
        return dataSchema;
    }

    public static void setPartitionColumnTypes (StructType dataSchema,int[] baseColumnMap, StructType tableSchema){

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

    /*
     if the external table is partitioned, its partitioned columns will be placed after all non-partitioned columns in StructField[] schema
     sort the columns so that partitioned columns are in their correct place
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
        String[] files = ImportUtils.getFileSystem(location).getExistingFiles(location, "*");
        return ((files.length == 0) || (files.length == 1 && "_SUCCESS".equals(truncateFileNameFromFullPath(files[0]))));

    }

    public static boolean isExisting(String location) throws Exception {
        FileInfo fileInfo = ImportUtils.getFileSystem(location).getInfo(location);
        return  fileInfo.exists();

    }

    public static String truncateFileNameFromFullPath(String file)
    {
        return file.substring(file.lastIndexOf(File.separator) + 1);
    }
}
