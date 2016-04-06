package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.util.Collections;

/**
 *
 * Function for parsing CSV files that are splittable by Hadoop.  The tokenizer swaps in and out
 * the line to be tokenized.
 *
 * Special attention should be paid to permissive execution of the OperationContext.  This occurs
 * during imports so that failures are <i>handled</i>.
 *
 *
 */
@NotThreadSafe
public class FileFunction extends AbstractFileFunction<String> {
    boolean initialized = false;
    MutableCSVTokenizer tokenizer;
    public FileFunction() {
        super();
    }
    public FileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                        String dateTimeFormat, String timestampFormat, OperationContext operationContext) {
        super(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat,
                dateTimeFormat, timestampFormat, operationContext);
    }

    /**
     *
     * Call Method for parsing the string into either a singleton List with a LocatedRow or
     * an empty list.
     *
     * @param s
     * @return
     * @throws Exception
     */
    @Override
    public Iterable<LocatedRow> call(final String s) throws Exception {
        if (operationContext.isFailed())
            return Collections.EMPTY_LIST;
        if (!initialized) {
            Reader reader = new StringReader(s);
            checkPreference();
            tokenizer= new MutableCSVTokenizer(reader,preference);
            initialized = true;
        }
        try {
            tokenizer.setLine(s);
            LocatedRow lr =  call(tokenizer.read());
            return lr==null?Collections.EMPTY_LIST:Collections.singletonList(lr);
        } catch (Exception e) {
            if (operationContext.isPermissive()) {
                operationContext.recordBadRecord(e.getLocalizedMessage(), e);
                return Collections.EMPTY_LIST;
            }
            throw StandardException.plainWrapException(e);
        }
    }
}