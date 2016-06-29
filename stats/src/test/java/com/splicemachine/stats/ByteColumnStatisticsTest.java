package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.collector.ByteColumnStatsCollector;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.estimate.ByteDistribution;
import com.splicemachine.stats.estimate.Distribution;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;

/**
 * @author Scott Fines
 *         Date: 6/26/15
 */
public class ByteColumnStatisticsTest{

    @Test
    public void testCanMerge() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1);
        byteColumnStatsCollector.update((byte)13);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        byteColumnStatsCollector =ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1,2l);

        ColumnStatistics<Byte> merge=build.merge(byteColumnStatsCollector.build());
        Assume.assumeTrue("Unexpected Data type",merge instanceof ByteColumnStatistics);
        //noinspection ConstantConditions
        build = (ByteColumnStatistics)merge;

        Assert.assertEquals("Incorrect nonNull count",4l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",0l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",2l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",3l,build.minCount());
        Assert.assertEquals("Incorrect minValue!",(byte)1,build.min());
        Assert.assertEquals("Incorrect maxValue!",(byte)13,build.max());

        long[] correct = new long[256];
        correct[129] = 3l;
        correct[13+128] = 1l;
        assertCorrectForAllValues(correct,build);
    }

    @Test
    public void canEncodeAndDecode() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        @SuppressWarnings("AccessStaticViaInstance") Encoder<ByteColumnStatistics> encoder=build.encoder();
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        encoder.encode(build,dos);

        dos.flush();
        build=encoder.decode(new DataInputStream(new ByteArrayInputStream(baos.toByteArray())));

        Assert.assertEquals("Incorrect nonNull count",1l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",0l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",1l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",1l,build.minCount());
        Assert.assertEquals("Incorrect minValue!",(byte)1,build.min());
        Assert.assertEquals("Incorrect maxValue!",(byte)1,build.max());

        long[] correct = new long[256];
        correct[129] = 1l;
        assertCorrectForAllValues(correct,build);
    }

    @Test
    public void testClone() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        //now clone me
        ColumnStatistics<Byte> clone=build.getClone();
        Assume.assumeTrue("Unexpected cloned type: test unable to run",clone instanceof ByteColumnStatistics);
        //noinspection ConstantConditions
        build = (ByteColumnStatistics)clone;

        Assert.assertEquals("Incorrect nonNull count",1l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",0l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",1l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",1l,build.minCount());
        Assert.assertEquals("Incorrect minValue!",(byte)1,build.min());
        Assert.assertEquals("Incorrect maxValue!",(byte)1,build.max());

        long[] correct = new long[256];
        correct[129] = 1l;
        assertCorrectForAllValues(correct,build);
    }

    @Test
    public void testIsCorrectForEmpty() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        Assert.assertEquals("Incorrect nonNull count",0l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",0l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",0l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",0l,build.minCount());

        long[] correct = new long[256];
        assertCorrectForAllValues(correct,build);
    }

    @Test
    public void testCorrectForASingleValue() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        Assert.assertEquals("Incorrect nonNull count",1l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",0l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",1l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",1l,build.minCount());
        Assert.assertEquals("Incorrect minValue!",(byte)1,build.min());
        Assert.assertEquals("Incorrect maxValue!",(byte)1,build.max());

        long[] correct = new long[256];
        correct[129] = 1l;
        assertCorrectForAllValues(correct,build);
    }

    @Test
    public void testCorrectForMultipleValues() throws Exception{
        ByteColumnStatsCollector byteColumnStatsCollector=ColumnStatsCollectors.byteCollector(0,5);
        byteColumnStatsCollector.update((byte)1);
        byteColumnStatsCollector.update((byte) 1, 2);
        byteColumnStatsCollector.update((byte) 13);
        byteColumnStatsCollector.updateNull(3);
        byteColumnStatsCollector.updateSize(4);
        ByteColumnStatistics build=byteColumnStatsCollector.build();

        Assert.assertEquals("Incorrect nonNull count",4l,build.nonNullCount());
        Assert.assertEquals("Incorrect null count",3l,build.nullCount());
        Assert.assertEquals("Incorrect cardinality!",2l,build.cardinality());
        Assert.assertEquals("Incorrect minCount!",3l,build.minCount());
        Assert.assertEquals("Incorrect minValue!",(byte)1,build.min());
        Assert.assertEquals("Incorrect maxValue!",(byte)13,build.max());
        Assert.assertEquals("Incorrect avgColumnWidth", 1, build.avgColumnWidth());

        long[] correct = new long[256];
        correct[129] = 3l;
        correct[13+128] = 1l;
        assertCorrectForAllValues(correct,build);
    }

    private void assertCorrectForAllValues(long[] correct,ByteColumnStatistics build){
        Distribution<Byte> distribution=build.getDistribution();
        Assume.assumeTrue("Unexpected distribution type!",distribution instanceof ByteDistribution);
        @SuppressWarnings("ConstantConditions") ByteDistribution bd = (ByteDistribution)distribution;
        for(byte i=Byte.MIN_VALUE;i<Byte.MAX_VALUE;i++){
            long cor = correct[i+128];
            long actual=bd.selectivity(i);
            Assert.assertEquals("Incorrect selectivity for byte "+ i,cor,actual);
        }
    }
}
