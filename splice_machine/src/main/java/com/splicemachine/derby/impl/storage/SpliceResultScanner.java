package com.splicemachine.derby.impl.storage;

import org.apache.derby.iapi.error.StandardException;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 10/29/13
 */
public interface SpliceResultScanner extends com.splicemachine.hbase.MeasuredResultScanner {

    void open() throws IOException, StandardException;

}
