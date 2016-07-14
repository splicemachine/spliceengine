/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;
import org.supercsv.quote.ColumnQuoteMode;

import java.io.*;

/**
 * Constructs/configures CsvListWriter objects used during export.
 */
public class ExportCSVWriterBuilder {

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