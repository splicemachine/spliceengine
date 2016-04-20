package com.splicemachine.derby.ddl;

import com.splicemachine.concurrent.TickingClock;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.utils.Sleeper;

import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Date: 9/8/15
 */
class IncrementingClock implements TickingClock,Sleeper{
    private int currentTime;

    public IncrementingClock(int startNanoes){
        this.currentTime=startNanoes;
    }

    @Override
    public long tickMillis(long millis){
        this.currentTime+=TimeUnit.MILLISECONDS.toNanos(millis);
        return currentTime;
    }

    @Override
    public long tickNanos(long nanos){
        this.currentTime+=nanos;
        return currentTime;
    }

//    @Override
    public void sleep(long time,TimeUnit unit) throws InterruptedException{
       tickNanos(unit.toNanos(time));
    }

    @Override
    public void sleep(long wait) throws InterruptedException{
        sleep(wait,TimeUnit.MILLISECONDS);
    }

    @Override
    public TimeView getSleepStats(){
        return Metrics.noOpTimeView();
    }

    @Override
    public long currentTimeMillis(){
        return TimeUnit.NANOSECONDS.toMillis(currentTime);
    }

    @Override
    public long nanoTime(){
        return currentTime;
    }
}
