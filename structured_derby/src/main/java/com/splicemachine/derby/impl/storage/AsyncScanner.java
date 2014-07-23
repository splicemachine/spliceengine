package com.splicemachine.derby.impl.storage;

import org.hbase.async.KeyValue;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 7/22/14
 */
public interface AsyncScanner extends SpliceResultScanner{
    List<KeyValue> nextKeyValues() throws Exception;
}
