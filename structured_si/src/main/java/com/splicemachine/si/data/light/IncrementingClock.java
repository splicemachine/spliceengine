package com.splicemachine.si.data.light;

public class IncrementingClock implements Clock {
    private long time = 0;

    public IncrementingClock() {
    }

    public IncrementingClock(long time) {
        this.time = time;
    }

    @Override
    public long getTime() {
        synchronized (this) {
            time = time + 1;
            return time - 1;
        }
    }
}
