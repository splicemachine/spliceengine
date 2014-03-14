package com.splicemachine.stats;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;

import com.splicemachine.hbase.CellUtils;

/**
 * @author Scott Fines
 *         Date: 1/23/14
 */
public class StatUtils {

    public static void countBytes(Counter counter, Result... results) {
        if (counter.isActive()) {
            long bytes = 0l;
            for (Result result : results) {
                for (Cell kv : result.rawCells()) {
                    bytes += CellUtils.getBuffer(kv).length;
                }
            }
            counter.add(bytes);
        }
    }

}
