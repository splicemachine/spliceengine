package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDecimal;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Test;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;

import static org.junit.Assert.assertEquals;

public class ExportExecRowWriterTest {

    @Test
    public void writeRow_withNullValue() throws IOException, StandardException {

        // given
        StringWriter writer = new StringWriter(100);
        CsvListWriter csvWriter = new CsvListWriter(writer, CsvPreference.EXCEL_PREFERENCE);
        ExportExecRowWriter execRowWriter = new ExportExecRowWriter(csvWriter);

        // when
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789));
        execRowWriter.writeRow(build("AAA", "BBB", null, "DDD", "EEE", 111.123456789, 222.123456789));   // null!
        execRowWriter.writeRow(build("AAA", "BBB", "CCC", "DDD", "EEE", 111.123456789, 222.123456789));
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


}