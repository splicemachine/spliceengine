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
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.TypeId;
import org.supercsv.io.CsvListWriter;

import java.io.Closeable;
import java.io.IOException;
import java.math.BigDecimal;
import java.text.NumberFormat;

import static splice.com.google.common.base.Preconditions.checkNotNull;

/**
 * Writes ExecRows to a CSVWriter
 */
public class ExportExecRowWriter implements Closeable {

    private CsvListWriter csvWriter;
    private NumberFormat decimalFormat = NumberFormat.getInstance();

    public ExportExecRowWriter(CsvListWriter csvWriter) {
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
