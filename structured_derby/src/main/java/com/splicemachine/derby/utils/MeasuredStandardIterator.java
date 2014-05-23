package com.splicemachine.derby.utils;

import com.splicemachine.stats.TimeView;

/**
 * Created by jyuan on 5/23/14.
 */
public interface MeasuredStandardIterator<T> extends StandardIterator<T> {
    TimeView getRemoteReadTime();

    long getRemoteBytesRead();

    long getRemoteRowsRead();
}
