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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import org.apache.spark.sql.types.StructField;
import org.junit.Test;
import org.supercsv.io.CsvListWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.Types;

import static org.junit.Assert.assertEquals;

public class ExportCSVWriterBuilderTest {

    private ExportCSVWriterBuilder csvWriterBuilder = new ExportCSVWriterBuilder();

    private void initColumn(ResultColumnDescriptor[] columns, int index, int type) {
        columns[index] = new ResultColumnDescriptor() {
            @Override
            public DataTypeDescriptor getType() {
                return DataTypeDescriptor.getBuiltInDataTypeDescriptor(type, true);
            }

            @Override
            public StructField getStructField() {
                return null;
            }

            @Override
            public String getName() {
                return null;
            }

            @Override
            public String getSourceSchemaName() {
                return null;
            }

            @Override
            public String getSourceTableName() {
                return null;
            }

            @Override
            public boolean updatableByCursor() {
                return false;
            }

            @Override
            public int getColumnPosition() {
                return 0;
            }

            @Override
            public boolean isAutoincrement() {
                return false;
            }

            @Override
            public boolean hasGenerationClause() {
                return false;
            }
        };
    }

    public void writeToCsv(ByteArrayOutputStream byteStream, ExportParams exportParams) throws IOException {
        ResultColumnDescriptor[] columns = new ResultColumnDescriptor[5];
        initColumn(columns, 0, Types.VARCHAR);
        initColumn(columns, 1, Types.CHAR);
        initColumn(columns, 2, Types.CHAR);
        initColumn(columns, 3, Types.CHAR);
        initColumn(columns, 4, Types.INTEGER);

        // when
        CsvListWriter csvWriter = csvWriterBuilder.build(byteStream, exportParams, columns);
        csvWriter.write("a1", "b1", "c1", "d1", 1);
        csvWriter.write("a2", "b 2", "c2", "d2", 2);      // space in field
        csvWriter.write("a3", "b3", "c3", "d,3", 3);      // comma in field
        csvWriter.write("a\n4", "b4", "c4", "d4", 4);     // newline in field
        csvWriter.write("a5", "b\"5", "c5", "d5", 5);     // quote in field
        csvWriter.write("a6", "b6", "c6\u1272", "d6", 6); // multi-byte unicode char in field
        csvWriter.close();
    }

    @Test
    public void buildCVSWriter() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ExportParams exportParams = ExportParams.withDirectory("/tmp");
        writeToCsv(byteStream, exportParams);

        // then
        assertEquals("" +
                        "a1,b1,c1,d1,1\n" +
                        "a2,b 2,c2,d2,2\n" +
                        "a3,b3,c3,\"d,3\",3\n" +
                        "\"a\n" +
                        "4\",b4,c4,d4,4\n" +
                        "a5,\"b\"\"5\",c5,d5,5\n" +
                        "a6,b6,c6ቲ,d6,6\n",
                new String(byteStream.toByteArray(), "UTF-8"));
    }

    @Test
    public void buildCSVWriterAllQuotes() throws IOException, StandardException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ExportParams exportParams = ExportParams.withDirectory("/tmp");
        exportParams.setQuoteMode("always");
        writeToCsv(byteStream, exportParams);

        // then
        assertEquals("" +
                        "\"a1\",\"b1\",\"c1\",\"d1\",1\n" +
                        "\"a2\",\"b 2\",\"c2\",\"d2\",2\n" +
                        "\"a3\",\"b3\",\"c3\",\"d,3\",3\n" +
                        "\"a\n" +
                        "4\",\"b4\",\"c4\",\"d4\",4\n" +
                        "\"a5\",\"b\"\"5\",\"c5\",\"d5\",5\n" +
                        "\"a6\",\"b6\",\"c6ቲ\",\"d6\",6\n",
                new String(byteStream.toByteArray(), "UTF-8"));
    }
}
