package com.splicemachine.derby.stream.output;

import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.function.SpliceFunction2;

import java.io.OutputStream;
import java.util.Iterator;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface ExportDataSetWriterBuilder<V> {

    ExportDataSetWriterBuilder<V> directory(String directory);

    <Op extends SpliceOperation> ExportDataSetWriterBuilder<V> exportFunction(SpliceFunction2<Op, OutputStream, Iterator<V>, Integer> exportFunction);

    DataSetWriter build();
}
