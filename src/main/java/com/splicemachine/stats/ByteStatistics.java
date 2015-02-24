package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface ByteStatistics extends ColumnStatistics<Byte>{

    byte min();

    byte max();
}
