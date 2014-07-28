package com.splicemachine.derby.impl.sql.execute.operations.window;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.AggregateContext;
import com.splicemachine.derby.impl.storage.SpliceResultScanner;
import com.splicemachine.derby.utils.StandardIterators;
import com.splicemachine.derby.utils.StandardPushBackIterator;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.PartitionAwarePushBackIterator;
import com.splicemachine.derby.utils.PartitionAwareIterator;
import org.apache.derby.iapi.types.DataValueDescriptor;

import java.io.IOException;

/**
 * Created by jyuan on 7/27/14.
 */
public class WindowFunctionIterator implements StandardIterator<ExecRow> {

    private WindowContext windowContext;
    private AggregateContext aggregateContext;
    private PairDecoder decoder;
    private SpliceResultScanner scanner;
    private DataValueDescriptor[] dvds;
    private PartitionAwarePushBackIterator<ExecRow> rowSource;
    private int[] partitionColumns;
    private WindowFunctionWorker windowFunctionWorker;
    private SpliceRuntimeContext runtimeContext;

    public WindowFunctionIterator() {

    }

    public WindowFunctionIterator(SpliceRuntimeContext runtimeContext,
                                  WindowContext windowContext,
                                  AggregateContext aggregateContext,
                                  SpliceResultScanner scanner,
                                  PairDecoder decoder,
                                  DataValueDescriptor[] dvds) {
        this.runtimeContext = runtimeContext;
        this.windowContext = windowContext;
        this.aggregateContext = aggregateContext;
        this.scanner = scanner;
        this.decoder = decoder;
        this.partitionColumns = windowContext.getPartitionColumns();
        this.dvds = dvds;
    }

    public boolean init() throws StandardException, IOException {
        boolean retval = false;
        PartitionAwareIterator<ExecRow> iterator = StandardIterators.wrap(scanner, decoder, partitionColumns, dvds);
        rowSource = new PartitionAwarePushBackIterator<ExecRow>(iterator);
        rowSource.open();
        ExecRow row = rowSource.next(runtimeContext);
        if (row != null) {
            rowSource.pushBack(row);
            initWindow();
            retval = true;
        }

        return retval;
    }

    private void initWindow() throws StandardException{
        if (windowFunctionWorker == null) {
            windowFunctionWorker = new WindowFunctionWorker(rowSource, windowContext.getWindowFrame(), aggregateContext);
        }
    }

    @Override
    public void open() throws StandardException, IOException{
        rowSource.open();
    }

    @Override
    public void close() throws StandardException, IOException{
        rowSource.close();
    }

    @Override
    public ExecRow next(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {
        return rowSource.next(runtimeContext);
    }
}
