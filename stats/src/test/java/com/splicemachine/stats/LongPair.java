package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 10/7/14
 */
public class LongPair{
    private long first;
    private long second;

    public LongPair(long first, long second) {
        this.first = first;
        this.second = second;
    }

    public long getFirst() { return first; }
    public void setFirst(long first) { this.first = first; }
    public long getSecond() { return second; }
    public void setSecond(long second) { this.second = second; }
}
