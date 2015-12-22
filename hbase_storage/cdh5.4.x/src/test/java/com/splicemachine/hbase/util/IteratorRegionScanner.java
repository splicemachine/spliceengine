package com.splicemachine.hbase.util;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;

import java.util.Iterator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/21/15
 */
public class IteratorRegionScanner extends AbstractIteratorRegionScanner{
    public IteratorRegionScanner(Iterator<Set<Cell>> kvs,Scan scan){
        super(kvs,scan);
    }
}
