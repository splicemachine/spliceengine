package com.splicemachine.si.impl;

import com.splicemachine.si.api.Clock;

/**
 * Implements the Clock interface as the current system time.
 */
public class SystemClock implements Clock {
    @Override
    public long getTime() {
        return System.currentTimeMillis();
    }
}
