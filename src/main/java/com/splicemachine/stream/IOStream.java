package com.splicemachine.stream;

import com.splicemachine.metrics.*;

/**
 * @author Scott Fines
 *         Date: 12/19/14
 */
public abstract class IOStream<T> extends ForwardingStream<T> implements MeasuredStream<T,IOStats> {
    private final Timer timer;
    private final Counter bytesCounter;
    public IOStream(Stream<T> delegate,MetricFactory metricFactory) {
        super(delegate);
        this.timer = metricFactory.newTimer();
        this.bytesCounter = metricFactory.newCounter();
    }

    @Override
    public T next() throws StreamException {
        timer.startTiming();
        T n = super.next();
        if(n==null)
            timer.stopTiming();
        else{
            timer.tick(1l);
            if(bytesCounter.isActive())
                countBytes(n,bytesCounter);
        }
        return n;
    }

    /**
     * Count the bytes contained in the returned field. This will only be called
     * if {@code bytesCounter} is active.
     *
     * @param n the element to count bytes for
     * @param bytesCounter an active bytes counter
     */
    protected abstract void countBytes(T n, Counter bytesCounter);

    @Override
    public <R> MeasuredStream<R,IOStats> transform(Transformer<T, R> transformer) {
        return new TransformingMeasuredStream<>(this,transformer);
    }

    @Override
    public MeasuredStream<T,IOStats> filter(Predicate<T> predicate) {
        return new FilteredMeasuredStream<>(this,predicate);
    }

    @Override
    public IOStats getStats() {
        return new BaseIOStats(timer.getTime(),bytesCounter.getTotal(),timer.getNumEvents());
    }

}
