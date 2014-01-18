package com.splicemachine.derby.impl.sql.execute.operations.framework;

import java.io.IOException;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.utils.StandardIterator;

public abstract class AbstractStandardIterator implements StandardIterator<GroupedRow> {
    protected final StandardIterator<ExecRow> source;
    
    public AbstractStandardIterator(StandardIterator<ExecRow> source) {
    	this.source = source;
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }
    
    @Override
    public void close() throws StandardException, IOException {
        source.close();
    }
    
}
