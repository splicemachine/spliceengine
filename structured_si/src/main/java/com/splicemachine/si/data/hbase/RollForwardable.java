package com.splicemachine.si.data.hbase;

/**
 * @author Jeff Cunningham
 *         Date: 5/2/14
 */
public interface RollForwardable {

    boolean checkRegionForRow(byte[] row);
}
