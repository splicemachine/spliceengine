package com.splicemachine.stats;

import com.splicemachine.stats.frequency.ShortFrequentElements;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface ShortColumnStatistics extends ColumnStatistics<Short>{
    short min();

    short max();

    ShortFrequentElements frequentElements();
}
