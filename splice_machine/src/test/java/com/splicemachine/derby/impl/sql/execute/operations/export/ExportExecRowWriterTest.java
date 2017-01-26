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

package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.*;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import org.junit.Test;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ExportExecRowWriterTest {

    @Test
    public void writeRow_withNullValue() throws IOException, StandardException {

        // given
        StringWriter writer = new StringWriter(100);
        CsvListWriter csvWriter = new CsvListWriter(writer, CsvPreference.EXCEL_PREFERENCE);
        ExportExecRowWriter execRowWriter = new ExportExecRowWriter(csvWriter);
        ResultColumnDescriptor[] columnDescriptors = columnDescriptors();

        // when
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789), columnDescriptors);
        execRowWriter.writeRow(build("AAA", "BBB", null, "DDD", "EEE", 111.123456789, 222.123456789), columnDescriptors);   // null!
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789), columnDescriptors);
        execRowWriter.close();

        // then
        assertEquals("" +
                "AAA,BBB,CCC,DDD,EEE,111.12,222.1234567\n" +
                "AAA,BBB,,DDD,EEE,111.12,222.1234567\n" +
                "AAA,BBB,CCC,DDD,EEE,111.12,222.1234567\n" +
                "", writer.toString());
    }

    private ExecRow build(String c1, String c2, String c3, String c4, String c5, double d1, double d2) throws StandardException {
        ExecRow row = new ValueRow(7);
        DataValueDescriptor[] rowValues = new DataValueDescriptor[7];
        rowValues[0] = new SQLVarchar(c1);
        rowValues[1] = new SQLVarchar(c2);
        rowValues[2] = new SQLVarchar(c3);
        rowValues[3] = new SQLVarchar(c4);
        rowValues[4] = new SQLVarchar(c5);

        rowValues[5] = new SQLDecimal(new BigDecimal(d1), 15, 2);
        rowValues[6] = new SQLDecimal(new BigDecimal(d2), 15, 7);

        row.setRowArray(rowValues);
        return row;
    }

    private ResultColumnDescriptor[] columnDescriptors() {
        ResultColumnDescriptor[] array = new ResultColumnDescriptor[7];

        array[0] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[1] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[2] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[3] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[4] = mockColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[5] = mockColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 2);
        array[6] = mockColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 7);

        return array;
    }

    private ResultColumnDescriptor mockColDesc(int formatId, int scale) {
        ResultColumnDescriptor mockVarCharColDesc = mock(ResultColumnDescriptor.class);
        DataTypeDescriptor mockType = mock(DataTypeDescriptor.class);
        TypeId mockTypeId = mock(TypeId.class);

        when(mockVarCharColDesc.getType()).thenReturn(mockType);
        when(mockType.getTypeId()).thenReturn(mockTypeId);
        when(mockType.getScale()).thenReturn(scale);
        when(mockTypeId.getTypeFormatId()).thenReturn(formatId);
        return mockVarCharColDesc;
    }


}