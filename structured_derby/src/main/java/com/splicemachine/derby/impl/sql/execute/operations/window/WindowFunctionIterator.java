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
import java.util.ArrayList;
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
    private PartitionAwarePushBackIterator<ExecRow> source;
    private int[] partitionColumns;
    private SpliceRuntimeContext runtimeContext;
    private ExecRow templateRow;
    FrameBuffer frameBuffer;

    public WindowFunctionIterator() {

    }

    public WindowFunctionIterator(SpliceRuntimeContext runtimeContext,
                                  WindowContext windowContext,
                                  AggregateContext aggregateContext,
                                  SpliceResultScanner scanner,
                                  PairDecoder decoder,
                                  ExecRow templateRow) {
        this.runtimeContext = runtimeContext;
        this.windowContext = windowContext;
        this.aggregateContext = aggregateContext;
        this.scanner = scanner;
        this.decoder = decoder;
        this.partitionColumns = windowContext.getPartitionColumns();
        this.templateRow = templateRow;
        this.dvds = templateRow.getRowArray();
    }

    public boolean init() throws StandardException, IOException {
        boolean retVal = false;
        PartitionAwareIterator<ExecRow> iterator = StandardIterators.wrap(scanner, decoder, partitionColumns, dvds);
        source = new PartitionAwarePushBackIterator<ExecRow>(iterator);
        source.open();
        ExecRow row = source.next(runtimeContext);
        if (row != null) {
            source.pushBack(row);
            initWindowFrame();
            retVal = true;
        }

        return retVal;
    }


    private void initWindowFrame() throws StandardException, IOException{
        if (frameBuffer == null) {
            frameBuffer = new FrameBuffer(runtimeContext, aggregateContext, source, windowContext, templateRow);
        }
        frameBuffer.init(source);
    }

    @Override
    public void open() throws StandardException, IOException{
        //rowSource.open();
    }

    @Override
    public void close() throws StandardException, IOException{
        //rowSource.close();
    }

    @Override
    public ExecRow next(SpliceRuntimeContext runtimeContext) throws StandardException, IOException {

        ExecRow row = frameBuffer.next();

        if (row == null) {
            // This is the end of one partition, peek the next row
            row = source.next(runtimeContext);
            if (row == null) {
                // This is the end of the region
                return null;
            }
            else {
                // init a new window buffer for the next partition
                source.pushBack(row);
                frameBuffer.init(source);
                row = frameBuffer.next();
            }

        }

        frameBuffer.move();

        return row;
    }
}
