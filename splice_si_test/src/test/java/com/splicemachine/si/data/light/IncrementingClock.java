package com.splicemachine.si.data.light;


import com.splicemachine.concurrent.Clock;

import java.util.concurrent.TimeUnit;

public class IncrementingClock implements Clock{
    private long time = 0;

    public IncrementingClock() {
    }

    public IncrementingClock(long time) {
        this.time = time;
    }

    @Override
    public long currentTimeMillis() {
        synchronized (this) {
            time = time + 1;
            return time - 1;
        }
    }

    @Override
    public long nanoTime(){
        return TimeUnit.MILLISECONDS.toNanos(currentTimeMillis());
    }

    public void delay(long delay) {
        time = time + delay;
    }
}
