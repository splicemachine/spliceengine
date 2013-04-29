package com.splicemachine.si.data.light;

import com.splicemachine.si.api.Clock;

public class ManualClock implements Clock {
    private long time = 0;

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public long getTime() {
        return time;
    }
}
