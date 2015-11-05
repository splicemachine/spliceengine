package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.load.SpliceCsvReader;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.*;
import java.util.Collections;
import java.util.Iterator;

/**
 * Created by jleach on 10/8/15.
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
    public Iterable<LocatedRow> call(final InputStream s) throws Exception {
        if (operationContext.isFailed())
            return Collections.EMPTY_LIST;
        checkPreference();

        return new Iterable<LocatedRow>() {

            @Override
            public Iterator<LocatedRow> iterator() {
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
                                        nextRow = call(spliceCsvReader.next());
                                        if (nextRow != null) {
                                            stale = true;
                                            hasNext = true;
                                            return true;
                                        }
                                    } catch (Exception e) {
                                        if (operationContext.isPermissive()) {
                                            operationContext.recordBadRecord("\n" + e.getLocalizedMessage());
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
                        stale = false;
                        return nextRow;
                    }

                    @Override
                    public void remove() {
                        throw new RuntimeException("not supported");
                    }
                };
            }
        };
    }
}
