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


package com.splicemachine.derby.stream.utils;

import com.google.common.io.Files;
import com.splicemachine.db.catalog.UUID;
import com.splicemachine.db.iapi.error.StandardException;
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

/**
 * Created by tgildersleeve on 8/2/17.
 */
public class ExternalTableUtils {

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
            StructField[] fields = new StructField[dvds.length];
            for (int i = 0; i < cdl.size(); ++i) {
                ColumnDescriptor columnDescriptor = cdl.get(i);
                DataTypeDescriptor dtd = columnDescriptor.getType();
                dvds[i] = dtd.getNull();
                fields[i] = dvds[i].getStructField(columnDescriptor.getColumnName());
            }
            StructType schema = DataTypes.createStructType(fields);
            return schema;
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

    public static String getSqlTypeName( org.apache.spark.sql.types.DataType datatype) {
        if( datatype.toString().equals("StringType") ) {
            // todo: stringlength?
            return "CHAR/VARCHAR(x)";
        }
        else if( datatype.toString().equals("FloatType") ) {
            // Spark's FloatType is NOT a SQL FLOAT type.
            // what is meant is a 4-byte floating point value, which is a REAL in SQL.
            // see https://doc.splicemachine.com/sqlref_datatypes_float.html .
            return "REAL";
        }
        else return datatype.sql();
    }

    private static boolean addTypes(StructType types, StringBuilder sb, boolean first, String separator)
    {
        for( int i =0 ; i < types.fields().length; i++)
        {
            StructField f = types.fields()[i];
            if( !first ) {
                sb.append("," + separator + " ");
            }
            first = false;
            sb.append( f.name() + " ");
            sb.append( getSqlTypeName( f.dataType() ) );
            if( !f.nullable() )
                sb.append(" NOT NULL");
        }
        return first;
    }

    public static void getSuggestedSchema(StringBuilder sb, StructType externalSchema,
                                          StructType partitionSchema, String separator) {
        sb.append( "CREATE EXTERNAL TABLE T (" + separator + " " );
        boolean first = addTypes(externalSchema, sb, true, separator);
        if( partitionSchema != null && partitionSchema.size() > 0 ) {
            addTypes(partitionSchema, sb, first, separator);
            sb.append( " " + separator + ")" );

            sb.append( " PARTITIONED BY(" + separator + " ");
            for( int i =0 ; i < partitionSchema.fields().length; i++)
            {
                if( i > 0 ) sb.append( "," + separator + " ");
                sb.append( partitionSchema.fields()[i].name());
            }
        }
        sb.append( " " + separator + ")" );
    }

    public static String getExternalTableTypeFromPath(String path)
    {
        String filetype = Files.getFileExtension(path).toUpperCase();
        if( filetype.equals("CSV") || filetype.equals("TBL") )
            return "TEXTFILE";
        else
            return filetype;
    }
}
