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
import com.splicemachine.derby.impl.load.SpliceCsvReader;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.utils.BooleanList;

import java.io.*;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 *
 *
 */
    public class StreamFileFunction extends AbstractFileFunction<InputStream> {
    private String charset;

    public StreamFileFunction() {
        super();
    }
    public StreamFileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                        String dateTimeFormat, String timestampFormat, String charset, OperationContext operationContext) {
        super(characterDelimiter,columnDelimiter,execRow,columnIndex,timeFormat,
                dateTimeFormat,timestampFormat,operationContext);
        assert charset != null;
        this.charset = charset;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(charset);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        charset = in.readUTF();
    }

    @Override
    public Iterator<LocatedRow> call(final InputStream s) throws Exception {
        if (operationContext.isFailed())
            return Collections.<LocatedRow>emptyList().iterator();
        checkPreference();

        return new Iterator<LocatedRow>() {
                    private LocatedRow nextRow;
                    private boolean initialized = false;
                    private Reader reader;
                    private SpliceCsvReader spliceCsvReader;
                    private boolean hasNext = true;
                    private boolean stale = false;
                    @Override
                    public boolean hasNext() {
                            if (!hasNext || stale)
                                return hasNext;
                            try {
                                if (!initialized) {
                                    reader = new BufferedReader(new InputStreamReader(s,charset));
                                    spliceCsvReader = new SpliceCsvReader(reader, preference);
                                    initialized = true;
                                }
                                while (true) {
                                    try {
                                        if (!spliceCsvReader.hasNext()) {
                                            reader.close();
                                            hasNext = false;
                                            return false;
                                        }
                                        List<String> next=spliceCsvReader.next();
                                        BooleanList quotedColumns = spliceCsvReader.nextQuotedColumns();
                                        nextRow = call(next,quotedColumns);
                                        if (nextRow != null) {
                                            stale = true;
                                            hasNext = true;
                                            return true;
                                        }
                                    } catch (Exception e) {
                                        if (operationContext.isPermissive()) {
                                            operationContext.recordBadRecord(e.getLocalizedMessage(), e);
                                        } else
                                            throw StandardException.plainWrapException(e);
                                    }
                                }
                            } catch (Exception ioe) {
                                throw new RuntimeException("Terminal, should have been caught", ioe);
                            }
                    }

                    @Override
                    public LocatedRow next() {
                        if(!hasNext()) throw new NoSuchElementException();
                        stale = false;
                        return nextRow;
                    }

                    @Override
                    public void remove() {
                        throw new RuntimeException("not supported");
                    }
                };
            }
    }
