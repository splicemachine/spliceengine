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


package com.splicemachine.derby.stream.utils;

import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.iapi.sql.dictionary.*;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.jdbc.SpliceTransactionResourceImpl;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.impl.driver.SIDriver;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
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
        for (int pos:partitionColumnMap) {
            partitionedTableFields[index++] = tableFields[pos];
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
}
