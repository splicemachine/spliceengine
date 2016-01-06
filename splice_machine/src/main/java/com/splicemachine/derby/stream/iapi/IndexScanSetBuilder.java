package com.splicemachine.derby.stream.iapi;

/**
 * @author Scott Fines
 *         Date: 1/8/16
 */
public interface IndexScanSetBuilder<V> extends ScanSetBuilder<V>{

    IndexScanSetBuilder<V> indexColToMainColPosMap(int[] colPosMap);
}
