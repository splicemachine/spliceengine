package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.ColumnQuoteMode;

import java.io.*;

/**
 * Constructs/configures CsvListWriter objects used during export.
 */
class ExportCSVWriterBuilder {

    /* On a local 4-node cluster varying this buffer size by 3 orders of magnitude had almost no effect. */
    private static final int WRITE_BUFFER_SIZE_BYTES = 16 * 1024;

    public CsvListWriter build(OutputStream outputStream, ExportParams exportParams) throws IOException {
        OutputStreamWriter stream = new OutputStreamWriter(outputStream, exportParams.getCharacterEncoding());
        Writer writer = new BufferedWriter(stream, WRITE_BUFFER_SIZE_BYTES);
        CsvPreference preference = new CsvPreference.Builder(
                exportParams.getQuoteChar(),
                exportParams.getFieldDelimiter(),
                exportParams.getRecordDelimiter())
                .useQuoteMode(new ColumnQuoteMode())
                .build();

        return new CsvListWriter(writer, preference);
    }

}