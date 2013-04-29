package com.splicemachine.si.impl;

import com.splicemachine.si.api.Clock;

public class SystemClock implements Clock {
    @Override
    public long getTime() {
        return System.currentTimeMillis();
    }
}
