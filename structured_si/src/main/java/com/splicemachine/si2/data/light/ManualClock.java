package com.splicemachine.si2.data.light;

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
