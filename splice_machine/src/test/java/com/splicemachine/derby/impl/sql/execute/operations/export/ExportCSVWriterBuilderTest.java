package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.junit.Test;
import org.supercsv.io.CsvListWriter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;

public class ExportCSVWriterBuilderTest {

    private ExportCSVWriterBuilder csvWriterBuilder = new ExportCSVWriterBuilder();

    @Test
    public void buildCVSWriter() throws IOException {

        // given
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        ExportParams exportParams = ExportParams.withDirectory("/tmp");

        // when
        CsvListWriter csvWriter = csvWriterBuilder.build(byteStream, exportParams);
        csvWriter.write(new String[]{"a1", "b1", "c1", "d1"});
        csvWriter.write(new String[]{"a2", "b 2", "c2", "d2"});      // space in field
        csvWriter.write(new String[]{"a3", "b3", "c3", "d,3"});      // comma in field
        csvWriter.write(new String[]{"a\n4", "b4", "c4", "d4"});     // newline in field
        csvWriter.write(new String[]{"a5", "b\"5", "c5", "d5"});     // quote in field
        csvWriter.write(new String[]{"a5", "b5", "c5\u1272", "d5"}); // multi-byte unicode char in field
        csvWriter.close();

        // then
        assertEquals("" +
                        "a1,b1,c1,d1\n" +
                        "a2,b 2,c2,d2\n" +
                        "a3,b3,c3,\"d,3\"\n" +
                        "\"a\n" +
                        "4\",b4,c4,d4\n" +
                        "a5,\"b\"\"5\",c5,d5\n" +
                        "a5,b5,c5ቲ,d5\n",
                new String(byteStream.toByteArray(), "UTF-8"));

    }

}