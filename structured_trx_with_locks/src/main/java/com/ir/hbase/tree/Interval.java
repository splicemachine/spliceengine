package com.ir.hbase.tree;

import com.google.common.collect.Ordering;

public class Interval<T>
{
    public final Comparable min;
    public final Comparable max;
    public final T Data;

    public Interval(Comparable min, Comparable max)
    {
        this.min = min;
        this.max = max;
        this.Data = null;
    }

    public Interval(Comparable min, Comparable max, T data)
    {
        this.min = min;
        this.max = max;
        this.Data = data;
    }

    public boolean encloses(Interval interval)
    {
        return (this.min.compareTo(interval.min) <= 0
                && this.max.compareTo(interval.max) >= 0);
    }

    public boolean contains(Comparable point)
    {
        return (this.min.compareTo(point) <= 0
                && this.max.compareTo(point) >= 0);
    }

    public boolean intersects(Interval interval)
    {
        return this.contains(interval.min) || this.contains(interval.max);
    }


    public static final Ordering<Interval> minOrdering = new Ordering<Interval>()
    {
        public int compare(Interval interval, Interval interval1)
        {
            return interval.min.compareTo(interval1.min);
        }
    };

    public static final Ordering<Interval> maxOrdering = new Ordering<Interval>()
    {
        public int compare(Interval interval, Interval interval1)
        {
            return interval.max.compareTo(interval1.max);
        }
    };

    public String toString()
    {
        return String.format("Interval(%s, %s)", min, max);
    }
}
