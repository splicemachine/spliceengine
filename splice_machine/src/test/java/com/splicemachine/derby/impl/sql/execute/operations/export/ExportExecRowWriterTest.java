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
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.compile.CompilerContext;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.junit.Test;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportExecRowWriterTest {

    private String writeRowImpl(int floatingPointDataType, String timestampFormat) throws StandardException, IOException {
        // given
        StringWriter writer = new StringWriter(100);
        CsvListWriter csvWriter = new CsvListWriter(writer, CsvPreference.EXCEL_PREFERENCE);
        ExportExecRowWriter execRowWriter = new ExportExecRowWriter(csvWriter, floatingPointDataType, timestampFormat);
        ResultColumnDescriptor[] columnDescriptors = columnDescriptors();

        // when
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789, 0,
                new Timestamp(1970 - 1900, 1 - 1, 1, 0, 0, 0, 0)), columnDescriptors);
        execRowWriter.writeRow(build("AAA", "BBB", null, "DDD", "EEE", 111.123456789, 222.123456789, 1234.1,
                new Timestamp(2020 - 1900, 12 - 1, 16, 16, 11, 39, 0)), columnDescriptors);   // null!
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789, -0.12354,
                new Timestamp(1975 - 1900, 2 - 1, 5, 6, 25, 9, 900000000)), columnDescriptors);
        execRowWriter.close();

        // then
        return writer.toString();
    }

    @Test
    public void writeRow_withNullValue() throws IOException, StandardException {
        assertEquals("" +
                    "AAA,BBB,CCC,DDD,EEE,111.12,222.1234568,0.0,1970-01-01 00:00:00.000000000\n" +
                    "AAA,BBB,,DDD,EEE,111.12,222.1234568,1234.1,2020-12-16 16:11:39.000000000\n" +
                    "AAA,BBB,CCC,DDD,EEE,111.12,222.1234568,-0.12354,1975-02-05 06:25:09.900000000\n",
                writeRowImpl(FloatingPointDataType.PLAIN, CompilerContext.DEFAULT_TIMESTAMP_FORMAT));
    }

    @Test
    public void writeRow_withNullValue_normalizedFloatingPoint_timestampFormat() throws IOException, StandardException {
        assertEquals("" +
                    "AAA,BBB,CCC,DDD,EEE,111.12,222.1234568,0E0,1970/01/01 00:00:00.0000\n" +
                    "AAA,BBB,,DDD,EEE,111.12,222.1234568,1.2341E3,2020/12/16 16:11:39.0000\n" +
                    "AAA,BBB,CCC,DDD,EEE,111.12,222.1234568,-1.2354E-1,1975/02/05 06:25:09.9000\n",
                writeRowImpl(FloatingPointDataType.NORMALIZED, "yyyy/MM/dd HH:mm:ss.SSSS"));
    }

    private ExecRow build(String c1, String c2, String c3, String c4, String c5, double d1, double d2, double d3, Timestamp t1) throws StandardException {
        ExecRow row = new ValueRow(9);
        DataValueDescriptor[] rowValues = new DataValueDescriptor[9];
        rowValues[0] = new SQLVarchar(c1);
        rowValues[1] = new SQLVarchar(c2);
        rowValues[2] = new SQLVarchar(c3);
        rowValues[3] = new SQLVarchar(c4);
        rowValues[4] = new SQLVarchar(c5);

        rowValues[5] = new SQLDecimal(new BigDecimal(d1), 15, 2);
        rowValues[6] = new SQLDecimal(new BigDecimal(d2), 15, 7);
        rowValues[7] = new SQLDouble(d3);
        rowValues[8] = new SQLTimestamp(t1);

        row.setRowArray(rowValues);
        return row;
    }

    private ResultColumnDescriptor[] columnDescriptors() {
        ResultColumnDescriptor[] array = new ResultColumnDescriptor[9];

        array[0] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0, false);
        array[1] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0, false);
        array[2] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0, false);
        array[3] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0, false);
        array[4] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0, false);
        array[5] = mockColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 2, false);
        array[6] = mockColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 7, false);
        array[7] = mockColDesc(StoredFormatIds.DOUBLE_TYPE_ID, 0, true);
        array[8] = mockColDesc(StoredFormatIds.TIMESTAMP_TYPE_ID, 0, false);

        return array;
    }

    private ResultColumnDescriptor mockColDesc(int formatId, int scale, boolean isFloatingPoint) {
        ResultColumnDescriptor mockVarCharColDesc = mock(ResultColumnDescriptor.class);
        DataTypeDescriptor mockType = mock(DataTypeDescriptor.class);
        TypeId mockTypeId = mock(TypeId.class);

        when(mockVarCharColDesc.getType()).thenReturn(mockType);
        when(mockType.getTypeId()).thenReturn(mockTypeId);
        when(mockType.getScale()).thenReturn(scale);
        when(mockTypeId.getTypeFormatId()).thenReturn(formatId);
        when(mockTypeId.isFloatingPointTypeId()).thenReturn(isFloatingPoint);
        return mockVarCharColDesc;
    }


}
