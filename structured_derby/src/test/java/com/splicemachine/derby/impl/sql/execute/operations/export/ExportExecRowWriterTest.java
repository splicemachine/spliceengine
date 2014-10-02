package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLVarchar;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.junit.Test;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringWriter;

import static org.junit.Assert.assertEquals;

public class ExportExecRowWriterTest {

    @Test
    public void writeRow_withNullValue() throws IOException, StandardException {

        // given
        StringWriter writer = new StringWriter(100);
        CsvListWriter csvWriter = new CsvListWriter(writer, CsvPreference.EXCEL_PREFERENCE);
        ExportExecRowWriter execRowWriter = new ExportExecRowWriter(csvWriter);

        // when
        execRowWriter.writeRow(build(new String[]{"AAA", "BBB", "CCC", "DDD", "EEE"}));
        execRowWriter.writeRow(build(new String[]{"AAA", "BBB", null, "DDD", "EEE"}));   // null!
        execRowWriter.writeRow(build(new String[]{"AAA", "BBB", "CCC", "DDD", "EEE"}));
        execRowWriter.close();

        // then
        assertEquals("" +
                "AAA,BBB,CCC,DDD,EEE\n" +
                "AAA,BBB,,DDD,EEE\n" +
                "AAA,BBB,CCC,DDD,EEE\n" +
                "", writer.toString());
    }

    private ExecRow build(String... cols) {
        ExecRow row = new ValueRow(cols.length);
        DataValueDescriptor[] rowValues = new DataValueDescriptor[cols.length];
        for (int i = 0; i < cols.length; i++) {
            rowValues[i] = new SQLVarchar(cols[i]);
        }
        row.setRowArray(rowValues);
        return row;
    }


}