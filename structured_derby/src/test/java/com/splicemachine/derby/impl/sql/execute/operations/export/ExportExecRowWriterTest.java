package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.StoredFormatIds;
import org.apache.derby.iapi.sql.ResultColumnDescriptor;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.*;
import org.apache.derby.impl.sql.execute.ValueRow;
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

        array[0] = mockVarcharColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[1] = mockVarcharColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[2] = mockVarcharColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[3] = mockVarcharColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[4] = mockVarcharColDesc(StoredFormatIds.VARCHAR_TYPE_ID, 0);
        array[5] = mockVarcharColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 2);
        array[6] = mockVarcharColDesc(StoredFormatIds.DECIMAL_TYPE_ID, 7);

        return array;
    }

    private ResultColumnDescriptor mockVarcharColDesc(int formatId, int scale) {
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