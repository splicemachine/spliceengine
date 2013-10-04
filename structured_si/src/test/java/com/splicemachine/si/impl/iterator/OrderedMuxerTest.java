package com.splicemachine.si.impl.iterator;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class OrderedMuxerTest {
    @Test
    public void muxing() {
        Assert.assertArrayEquals(new Integer[]{0, 1, 2, 4, 5, 20},
                mux(new Integer[][]{
                        {0, 2, 4},
                        {1, 5, 20}}));
    }

    @Test
    public void muxingOneEmpty() {
        Assert.assertArrayEquals(new Integer[]{0, 2, 4},
                mux(new Integer[][]{{0, 2, 4}}));
    }

    @Test
    public void muxingAllEmpty() {
        Assert.assertArrayEquals(new Integer[]{},
                mux(new Integer[][]{
                        {},
                        {},
                        {}}));
    }

    @Test
    public void muxingDups() {
        Assert.assertArrayEquals(new Integer[]{1, 2, 2, 2, 2, 2, 3},
                mux(new Integer[][]{
                        {2, 2, 2, 3},
                        {1, 2, 2}}));
    }

    @Test
    public void muxingUnordered() {
        Assert.assertArrayEquals("If the underlying streams are not ordered then the output is not ordered",
                new Integer[]{0, 2, 3, 4, 6, 8, 9, 5, 7, 1},
                mux(new Integer[][]{
                        {0, 2, 4, 6, 8},
                        {3, 9, 5, 7, 1}}));
    }

    final Comparator<Integer> comparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            if (o1 < o2) {
                return -1;
            } else if (o1 > o2) {
                return 1;
            } else {
                return 0;
            }
        }
    };

    final DataIDDecoder<Integer, Integer> decoder = new DataIDDecoder<Integer, Integer>() {
        @Override
        public Integer getID(Integer integer) {
            return integer;
        }
    };

    private Integer[] mux(Integer[][] sourceData) {
        return resultToArray(new OrderedMuxer(setupSources(sourceData), comparator, decoder));
    }

    static Integer[] resultToArray(Iterator<Integer> sequence) {
        List<Integer> result = new ArrayList<Integer>();
        while (sequence.hasNext()) {
            result.add(sequence.next());
        }
        return result.toArray(new Integer[result.size()]);
    }

    static List<Iterator<Integer>> setupSources(Integer[][] sourceData) {
        List<Iterator<Integer>> sources = new ArrayList<Iterator<Integer>>();
        for (int i = 0; i < sourceData.length; i++) {
            sources.add(Iterators.<Integer>forArray(sourceData[i]));
        }
        return sources;
    }
}
