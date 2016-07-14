/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.stats;

import com.splicemachine.encoding.Encoder;
import com.splicemachine.stats.collector.BooleanColumnStatsCollector;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.frequency.FrequentElements;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;

import java.io.*;

/**
 * @author Scott Fines
 *         Date: 6/26/15
 */
public class BooleanColumnStatisticsTest{

    @Test
    public void testMergeWorksProperly() throws Exception{
        BooleanColumnStatsCollector statsCollector=ColumnStatsCollectors.booleanCollector(0);
        statsCollector.update(Boolean.TRUE);
        statsCollector.update(false);
        statsCollector.update(null);
        BooleanColumnStatistics build=statsCollector.build();

        long correctTrueCount=1l;
        long correctFalseCount=1l;
        long correctNullCount=1l;
        long correctMinCount=1l;
        long correctCardinality=2l;
        Boolean expectedMinimum=Boolean.TRUE;
        Boolean expectedMaximum = Boolean.FALSE;

        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);

        //now merge with another
        statsCollector=ColumnStatsCollectors.booleanCollector(0);
        statsCollector.update(Boolean.TRUE);
        statsCollector.update(false);
        statsCollector.update(null);
        BooleanColumnStatistics build2=statsCollector.build();
        ColumnStatistics<Boolean> merge=build.merge(build2);
        Assume.assumeTrue("Merged is not of the expected type",merge instanceof BooleanColumnStatistics);
        correctTrueCount++;
        correctFalseCount++;
        correctNullCount++;
        correctMinCount++;

        build = (BooleanColumnStatistics)merge;
        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);
    }

    @Test
    public void testCanEncodeAndDecodeProperly() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.update(Boolean.TRUE);
        emptyStats.update(false);
        emptyStats.update(null);
        BooleanColumnStatistics build=emptyStats.build();

        long correctTrueCount=1l;
        long correctFalseCount=1l;
        long correctNullCount=1l;
        long correctMinCount=1l;
        long correctCardinality=2l;
        Boolean expectedMinimum=Boolean.TRUE;
        Boolean expectedMaximum = Boolean.FALSE;

        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);

        //now encode and decode it
        Encoder<BooleanColumnStatistics> encoder=build.encoder();
        ByteArrayOutputStream out=new ByteArrayOutputStream();
        DataOutput dO = new DataOutputStream(out);
        encoder.encode(build,dO);

        out.flush();
        DataInput di = new DataInputStream(new ByteArrayInputStream(out.toByteArray()));
        encoder.decode(di);

        //now check the clone
        ColumnStatistics<Boolean> clone = build.getClone();
        Assume.assumeTrue("Clone is not instance of BooleanColumnStatistics!",clone instanceof BooleanColumnStatistics);

        build = (BooleanColumnStatistics)clone;
        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);
    }

    @Test
    public void testCloneIsCorrect() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.update(Boolean.TRUE);
        emptyStats.update(false);
        emptyStats.update(null);
        BooleanColumnStatistics build=emptyStats.build();

        long correctTrueCount=1l;
        long correctFalseCount=1l;
        long correctNullCount=1l;
        long correctMinCount=1l;
        long correctCardinality=2l;
        Boolean expectedMinimum=Boolean.TRUE;
        Boolean expectedMaximum = Boolean.FALSE;

        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);

        //now check the clone
        ColumnStatistics<Boolean> clone = build.getClone();
        Assume.assumeTrue("Clone is not instance of BooleanColumnStatistics!",clone instanceof BooleanColumnStatistics);

        build = (BooleanColumnStatistics)clone;
        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);
    }

    @Test
    public void testCorrectForAllValues() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.update(Boolean.TRUE);
        emptyStats.update(false);
        emptyStats.update(null);
        BooleanColumnStatistics build=emptyStats.build();

        long correctTrueCount=1l;
        long correctFalseCount=1l;
        long correctNullCount=1l;
        long correctMinCount=1l;
        long correctCardinality=2l;
        Boolean expectedMinimum=Boolean.TRUE;
        Boolean expectedMaximum = Boolean.FALSE;

        assertCorrectStatistics(build,correctTrueCount,correctFalseCount,correctNullCount,correctMinCount,correctCardinality,expectedMinimum,expectedMaximum);
    }

    private void assertCorrectStatistics(BooleanColumnStatistics build,long correctTrueCount,long correctFalseCount,long correctNullCount,long correctMinCount,long correctCardinality,Boolean expectedMinimum,Boolean expectedMaximum){
        Assert.assertEquals("Incorrect minimum value!",expectedMinimum,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",expectedMaximum,build.maxValue());
        Assert.assertEquals("Incorrect nonNullCount!",correctTrueCount+correctFalseCount,build.nonNullCount());
        Assert.assertEquals("Incorrect nullCount!",correctNullCount,build.nullCount());
        Assert.assertEquals("Incorrect minCount",correctMinCount,build.minCount());
        Assert.assertEquals("Incorrect cardinality",correctCardinality,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",correctTrueCount,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",correctTrueCount,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",correctFalseCount,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect trueCount",correctFalseCount,build.falseCount().count());
    }

    @Test
    public void testMinValueCorrectForEmptyStatistics() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        BooleanColumnStatistics build=emptyStats.build();

        Assert.assertEquals("Incorrect minimum value!",Boolean.TRUE,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",Boolean.TRUE,build.maxValue());
        Assert.assertEquals("Incorrect nullCount!",0l,build.nullCount());
        Assert.assertEquals("Incorrect nonNullCount!",0l,build.nonNullCount());
        Assert.assertEquals("Incorrect minCount",0l,build.minCount());
        Assert.assertEquals("Incorrect cardinality",0,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",0,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",0,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.falseCount().count());
    }

    @Test
    public void testMinValueCorrectForOnlyFalse() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.update(Boolean.FALSE);
        emptyStats.update(false);
        BooleanColumnStatistics build=emptyStats.build();

        Assert.assertEquals("Incorrect minimum value!",Boolean.FALSE,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",Boolean.FALSE,build.maxValue());
        Assert.assertEquals("Incorrect count!",2,build.nonNullCount());
        Assert.assertEquals("Incorrect minCount",2,build.minCount());
        Assert.assertEquals("Incorrect cardinality",1,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",0,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",2,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect trueCount",2l,build.falseCount().count());
    }

    @Test
    public void testCorrectForOnlyTrue() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.update(Boolean.TRUE);
        emptyStats.update(true);
        BooleanColumnStatistics build=emptyStats.build();

        Assert.assertEquals("Incorrect minimum value!",Boolean.TRUE,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",Boolean.TRUE,build.maxValue());
        Assert.assertEquals("Incorrect count!",2l,build.nonNullCount());
        Assert.assertEquals("Incorrect minCount",2l,build.minCount());
        Assert.assertEquals("Incorrect cardinality",1,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",2,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",2l,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",0,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect falseCount",0l,build.falseCount().count());
    }

    @Test
    public void testProperlyUpdatesNullCount() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.updateNull();
        BooleanColumnStatistics build=emptyStats.build();

        Assert.assertEquals("Incorrect minimum value!",Boolean.TRUE,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",Boolean.TRUE,build.maxValue());
        Assert.assertEquals("Incorrect nonNullCount!",0l,build.nonNullCount());
        Assert.assertEquals("Incorrect nullCount!",1l,build.nullCount());
        Assert.assertEquals("Incorrect minCount",0l,build.minCount());

        Assert.assertEquals("Incorrect cardinality",0,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",0,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",0,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.falseCount().count());
    }

    @Test
    public void testProperlyUpdatesSize() throws Exception{
        BooleanColumnStatsCollector emptyStats=ColumnStatsCollectors.booleanCollector(0);
        emptyStats.updateSize(10);
        emptyStats.update(Boolean.TRUE);
        BooleanColumnStatistics build=emptyStats.build();


        Assert.assertEquals("Incorrect minimum value!",Boolean.TRUE,build.minValue());
        Assert.assertEquals("Incorrect maximum value!",Boolean.TRUE,build.maxValue());
        Assert.assertEquals("Incorrect nonNullCount!",1l,build.nonNullCount());
        Assert.assertEquals("Incorrect nullCount!",0l,build.nullCount());
        Assert.assertEquals("Incorrect minCount",1l,build.minCount());
        Assert.assertEquals("Incorrect avgColumnWidth!",10,build.avgColumnWidth());
        Assert.assertEquals("Incorrect cardinality",1,build.cardinality());

        FrequentElements<Boolean> booleanFrequentElements=build.topK();
        Assert.assertEquals("Incorrect trueCount",1,booleanFrequentElements.equal(Boolean.TRUE).count());
        Assert.assertEquals("Incorrect trueCount",1l,build.trueCount().count());
        Assert.assertEquals("Incorrect falseCount",0,booleanFrequentElements.equal(Boolean.FALSE).count());
        Assert.assertEquals("Incorrect trueCount",0l,build.falseCount().count());
    }
}
