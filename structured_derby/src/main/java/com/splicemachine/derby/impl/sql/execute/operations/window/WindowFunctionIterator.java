package com.splicemachine.derby.impl.sql.execute.operations.window;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.StandardIterator;

/**
 * Created by jyuan on 7/27/14.
 */
public class WindowFunctionIterator implements StandardIterator<ExecRow> {

    private final WindowFrameBuffer frameBuffer;

    public WindowFunctionIterator(WindowFrameBuffer frameBuffer) {
        this.frameBuffer = frameBuffer;
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
        return frameBuffer.next(runtimeContext);
    }
}
