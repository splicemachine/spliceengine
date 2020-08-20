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

package com.splicemachine.derby.stream.function;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataTypeDescriptor;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.VTIOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.BooleanList;
import org.apache.commons.collections.iterators.SingletonIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * Function for parsing CSV files that are splittable by Hadoop.  The tokenizer swaps in and out
 * the line to be tokenized.
 * <p>
 * Special attention should be paid to permissive execution of the OperationContext.  This occurs
 * during imports so that failures are <i>handled</i>.
 */
@NotThreadSafe
public class FileFunction extends AbstractFileFunction<String> {
    boolean initialized = false;
    MutableCSVTokenizer tokenizer;
    private boolean oneLineRecord;

    public FileFunction() {
        super();
    }

    public FileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                        String dateTimeFormat, String timestampFormat, boolean oneLineRecord, OperationContext operationContext) {
        super(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat,
                dateTimeFormat, timestampFormat, operationContext);
        this.oneLineRecord = oneLineRecord;
    }

    /**
     * Call Method for parsing the string into either a singleton List with a ExecRow or an empty list.
     *
     * @param s the input string, e.g. comma-separated.
     * @return Iterator on parsed row(s) of the string.
     */
    @Override
    public Iterator<ExecRow> call(final String s) throws Exception {
        if (operationContext.isFailed())
            return Collections.<ExecRow>emptyList().iterator();
        if (!initialized) {
            Reader reader = new StringReader(""); // no need to pass the string here, rely on setLine() method to
            // pass the string to CSV tokenizer, this allows us to immediately
            // get rid of the string after reading it.
            checkPreference();
            List<Integer> valueSizeHints = null;
            SpliceOperation op = operationContext.getOperation();
            if (op instanceof VTIOperation) { // Currently, only VTI can provide the result set data types.
                valueSizeHints = new ArrayList<>(execRow.nColumns());
                DataTypeDescriptor[] dvds = ((VTIOperation) op).getResultColumnTypes();
                for (DataTypeDescriptor dtd : dvds) {
                    valueSizeHints.add(dtd.getMaximumWidth());
                }
            }
            tokenizer = new MutableCSVTokenizer(reader, preference, oneLineRecord,
                    EngineDriver.driver().getConfiguration().getImportCsvScanThreshold(), valueSizeHints);
            initialized = true;
        }
        try {
            tokenizer.setLine(s);
            List<String> read = tokenizer.read();
            BooleanList quotedColumns = tokenizer.getQuotedColumns();
            ExecRow lr = call(read, quotedColumns);
            return lr == null ? Collections.<ExecRow>emptyList().iterator() : new SingletonIterator(lr);
        } catch (Exception e) {
            if (operationContext.isPermissive()) {
                operationContext.recordBadRecord(e.getLocalizedMessage(), e);
                return Collections.<ExecRow>emptyList().iterator();
            }
            throw StandardException.plainWrapException(e);
        }
    }
}
