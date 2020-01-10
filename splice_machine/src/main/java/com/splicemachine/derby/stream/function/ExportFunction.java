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

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportCSVWriterBuilder;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportExecRowWriter;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportOperation;
import com.splicemachine.derby.impl.sql.execute.operations.export.ExportParams;
import com.splicemachine.derby.stream.iapi.OperationContext;
import org.supercsv.io.CsvListWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Iterator;

/**
 * Created by jleach on 10/28/15.
 */
public class ExportFunction extends SpliceFunction2<ExportOperation, OutputStream, Iterator<ExecRow>, Integer> {
        public ExportFunction() {
        }

        public ExportFunction(OperationContext<ExportOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public Integer call(OutputStream outputStream, Iterator<ExecRow> locatedRowIterator) throws Exception {
            ExportOperation op = operationContext.getOperation();
            ExportExecRowWriter rowWriter = initializeRowWriter(outputStream, op.getExportParams());
            int count = 0;
            while (locatedRowIterator.hasNext()) {
                count++;
                rowWriter.writeRow(locatedRowIterator.next(), op.getSourceResultColumnDescriptors());
            }
            rowWriter.close();
            return count;
        }

    public static ExportExecRowWriter initializeRowWriter(OutputStream outputStream, ExportParams exportParams) throws IOException {
        CsvListWriter writer = new ExportCSVWriterBuilder().build(outputStream, exportParams);
        return new ExportExecRowWriter(writer);
    }

}
