/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.BooleanList;
import org.apache.commons.collections.iterators.SingletonIterator;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

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
    public Iterator<LocatedRow> call(final String s) throws Exception {
        if (operationContext.isFailed())
            return Collections.<LocatedRow>emptyList().iterator();
        if (!initialized) {
            Reader reader = new StringReader(s);
            checkPreference();
            tokenizer= new MutableCSVTokenizer(reader,preference);
            initialized = true;
        }
        try {
            tokenizer.setLine(s);
            List<String> read=tokenizer.read();
            BooleanList quotedColumns=tokenizer.getQuotedColumns();
            LocatedRow lr =  call(read,quotedColumns);
            return lr==null?Collections.<LocatedRow>emptyList().iterator():new SingletonIterator(lr);
        } catch (Exception e) {
            if (operationContext.isPermissive()) {
                operationContext.recordBadRecord(e.getLocalizedMessage(), e);
                return Collections.<LocatedRow>emptyList().iterator();
            }
            throw StandardException.plainWrapException(e);
        }
    }
}