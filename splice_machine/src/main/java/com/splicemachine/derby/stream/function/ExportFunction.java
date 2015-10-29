package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
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
public class ExportFunction extends SpliceFunction2<ExportOperation, OutputStream, Iterator<LocatedRow>, Integer> {
        public ExportFunction() {
        }

        public ExportFunction(OperationContext<ExportOperation> operationContext) {
            super(operationContext);
        }

        @Override
        public Integer call(OutputStream outputStream, Iterator<LocatedRow> locatedRowIterator) throws Exception {
            ExportOperation op = operationContext.getOperation();
            ExportExecRowWriter rowWriter = initializeRowWriter(outputStream, op.getExportParams());
            int count = 0;
            while (locatedRowIterator.hasNext()) {
                count++;
                LocatedRow lr = locatedRowIterator.next();
                rowWriter.writeRow(lr.getRow(), op.getSourceResultColumnDescriptors());
            }
            rowWriter.close();
            return count;
        }

    public static ExportExecRowWriter initializeRowWriter(OutputStream outputStream, ExportParams exportParams) throws IOException {
        CsvListWriter writer = new ExportCSVWriterBuilder().build(outputStream, exportParams);
        return new ExportExecRowWriter(writer);
    }

}
