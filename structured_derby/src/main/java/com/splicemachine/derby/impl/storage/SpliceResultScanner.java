package com.splicemachine.derby.impl.storage;

import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 10/29/13
 */
public interface SpliceResultScanner extends ResultScanner {

    void open() throws IOException, StandardException;
}
