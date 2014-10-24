package com.splicemachine.derby.impl.sql.execute.operations.export;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.SQLDecimal;
import org.supercsv.io.CsvListWriter;

import java.io.Closeable;
import java.io.IOException;
import java.text.NumberFormat;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Writes ExecRows to a CSVWriter
 */
class ExportExecRowWriter implements Closeable {

    private CsvListWriter csvWriter;
    private NumberFormat decimalFormat = NumberFormat.getInstance();

    ExportExecRowWriter(CsvListWriter csvWriter) {
        checkNotNull(csvWriter);
        this.csvWriter = csvWriter;
    }

    /**
     * Write one ExecRow.
     */
    public void writeRow(ExecRow execRow) throws IOException, StandardException {
        DataValueDescriptor[] rowArray = execRow.getRowArray();
        String[] stringRowArray = new String[rowArray.length];
        for (int i = 0; i < rowArray.length; i++) {
            DataValueDescriptor value = rowArray[i];
            // null
            if (value == null || value.isNull()) {
                stringRowArray[i] = null;
            }
            // decimal
            else if (value instanceof SQLDecimal) {
                int scale = ((SQLDecimal) value).getDecimalValueScale();
                decimalFormat.setMaximumFractionDigits(scale);
                stringRowArray[i] = decimalFormat.format(value.getDouble());
            }
            // everything else
            else {
                stringRowArray[i] = value.getString();
            }
        }
        csvWriter.write(stringRowArray);
    }

    /**
     * Will flush and close
     */
    @Override
    public void close() throws IOException {
        csvWriter.close();
    }

}
