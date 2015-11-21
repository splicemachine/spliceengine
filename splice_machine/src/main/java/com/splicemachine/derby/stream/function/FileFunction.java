package com.splicemachine.derby.stream.function;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.load.SpliceCsvReader;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import java.io.*;
import java.util.Collections;

/**
 * Created by jleach on 10/8/15.
 */
    public class FileFunction extends AbstractFileFunction<String> {
        public FileFunction() {
            super();
        }
        public FileFunction(String characterDelimiter, String columnDelimiter, ExecRow execRow, int[] columnIndex, String timeFormat,
                            String dateTimeFormat, String timestampFormat, OperationContext operationContext) {
            super(characterDelimiter, columnDelimiter, execRow, columnIndex, timeFormat,
                    dateTimeFormat, timestampFormat, operationContext);
        }

        @Override
        public Iterable<LocatedRow> call(String s) throws Exception {
            if (operationContext.isFailed())
                return Collections.EMPTY_LIST;
            Reader reader = null;
            SpliceCsvReader spliceCsvReader = null;
            try {
                reader = new StringReader(s);
                checkPreference();
                spliceCsvReader = new SpliceCsvReader(reader, preference);
                LocatedRow lr =  call(spliceCsvReader.read());
                return lr==null?Collections.EMPTY_LIST:Collections.singletonList(lr);
            } catch (Exception e) {
                if (operationContext.isPermissive()) {
                    operationContext.recordBadRecord("\n" + e.getLocalizedMessage() + "\n");
                    return Collections.EMPTY_LIST;
                }
                throw StandardException.plainWrapException(e);
            } finally {
                reader.close();
            }
        }
}
