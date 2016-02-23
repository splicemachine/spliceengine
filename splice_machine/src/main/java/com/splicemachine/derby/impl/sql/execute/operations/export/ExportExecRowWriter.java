package com.splicemachine.derby.impl.sql.execute.operations.export;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.sql.ResultColumnDescriptor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import org.supercsv.io.CsvListWriter;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
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
    public void writeRow(ExecRow execRow, ResultColumnDescriptor[] columnDescriptors) throws IOException, StandardException {
        DataValueDescriptor[] rowArray = execRow.getRowArray();
        String[] stringRowArray = new String[rowArray.length];
        for (int i = 0; i < rowArray.length; i++) {
            DataValueDescriptor value = rowArray[i];

            // null
            if (value == null || value.isNull()) {
                stringRowArray[i] = null;
            }

            // decimal -- We format the number in the CSV to have the same scale as the source decimal column type.
            // Apparently some tools (Ab Initio) cannot import from CSV a number "15" that is supposed
            // to be decimal(31, 7) unless the CSV contains exactly "15.0000000".
            else if (isDecimal(columnDescriptors[i])) {
                int scale = columnDescriptors[i].getType().getScale();
                decimalFormat.setMaximumFractionDigits(scale);
                decimalFormat.setMinimumFractionDigits(scale);
                decimalFormat.setGroupingUsed(false);
                BigDecimal valueObject = (BigDecimal) value.getObject();
                stringRowArray[i] = decimalFormat.format(valueObject);
            }

            // everything else
            else {
                stringRowArray[i] = value.getString();
            }
        }
        csvWriter.write(stringRowArray);
    }

    private boolean isDecimal(ResultColumnDescriptor columnDescriptor) {
        TypeId typeId = columnDescriptor.getType().getTypeId();
        return typeId != null && typeId.getTypeFormatId() == StoredFormatIds.DECIMAL_TYPE_ID;
    }

    /**
     * Will flush and close
     */
    @Override
    public void close() throws IOException {
        csvWriter.close();
    }

}
