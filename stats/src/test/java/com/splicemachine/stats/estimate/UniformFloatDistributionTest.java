package com.splicemachine.stats.estimate;

import com.carrotsearch.hppc.FloatArrayList;
import com.splicemachine.stats.FloatColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.collector.FloatColumnStatsCollector;
import com.splicemachine.stats.frequency.FloatFrequencyCounter;
import com.splicemachine.stats.frequency.FloatFrequentElements;
import com.splicemachine.stats.frequency.FrequencyCounters;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * @author Scott Fines
 *         Date: 6/25/15
 */
@SuppressWarnings("ForLoopReplaceableByForEach")
public class UniformFloatDistributionTest{

    @Test
    public void testSelectivityRemainsBounded() throws Exception{
        /*
         * The idea here is to ensure that the selectivity estimates that we provide don't violate
         * the invariants of falling within the range [0,totalCount()).
         */
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        float[] v=loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        Arrays.sort(v);
        for(int i=0;i<v.length;i++){
            float mi = v[i];
            long sel = distribution.selectivity(mi);

            Assert.assertTrue("negative selectivity!",sel>=0);
            Assert.assertTrue("overlarge selectivity!",sel<=v.length);
            Assert.assertTrue("overlarge selectivity!",sel<=distribution.totalCount());

            for(int j=i+1;j<v.length;j++){
                float ma = v[j];

                long rs=distribution.rangeSelectivity(mi,ma,true,true);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,true,false);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,false,true);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(mi,ma,false,false);
                Assert.assertTrue("negative selectivity: mi=<"+mi+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: mi=<"+mi+">, ma=<"+ma+">!:rs="+rs,rs<=v.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());
            }
        }
    }



    @Test(expected=ArithmeticException.class)
    public void testCannotGetSelectivityForNan() throws Exception{
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        distribution.selectivity(Float.NaN);
    }

    @Test
    public void testCannotGetRangeSelectivityForNanStop() throws Exception{
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        float[] v=loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        for(int i=0;i<v.length;i++){
            float d = v[i];
            try{
                distribution.rangeSelectivity(d,Float.NaN,true,true);
                Assert.fail("Was able to perform selectivity for ["+d+",Nan]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Float.NaN,true,false);
                Assert.fail("Was able to perform selectivity for ["+d+",Nan)!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Float.NaN,false,true);
                Assert.fail("Was able to perform selectivity for ("+d+",Nan]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(d,Float.NaN,false,false);
                Assert.fail("Was able to perform selectivity for ("+d+",Nan)!");
            }catch(ArithmeticException ignored){}
        }
    }

    @Test
    public void testCannotGetRangeSelectivityForNanStart() throws Exception{
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        float[] v=loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        for(int i=0;i<v.length;i++){
            float d = v[i];
            try{
                distribution.rangeSelectivity(Float.NaN,d,true,true);
                Assert.fail("Was able to perform selectivity for [NaN,"+d+"]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Float.NaN,d,true,false);
                Assert.fail("Was able to perform selectivity for [NaN,"+d+")!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Float.NaN,d,false,true);
                Assert.fail("Was able to perform selectivity for (NaN,"+d+"]!");
            }catch(ArithmeticException ignored){}

            try{
                distribution.rangeSelectivity(Float.NaN,d,false,false);
                Assert.fail("Was able to perform selectivity for (NaN,"+d+")!");
            }catch(ArithmeticException ignored){}
        }
    }

    @Test
    public void testPositiveInfinitySelectivityCorrect(){
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        float[] v=loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        float mi = Float.POSITIVE_INFINITY;
        long s = distribution.selectivity(mi);
        Assert.assertEquals("Did not return 0 for nonexistent numbers!",0l,s);
        float max = distribution.max();
        for(int j=0;j<v.length;j++){
            float ma = v[j];
            long rs=distribution.rangeSelectivity(ma,mi,true,true);
            long cs = distribution.rangeSelectivity(ma,max,true,true);
            Assert.assertEquals("selectivity of ["+ma+",Inf] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,true,false);
            cs = distribution.rangeSelectivity(ma,max,true,true);
            Assert.assertEquals("selectivity of ["+ma+",Inf) incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,false,true);
            cs = distribution.rangeSelectivity(ma,max,false,true);
            Assert.assertEquals("selectivity of ("+ma+",Inf] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(ma,mi,false,false);
            cs = distribution.rangeSelectivity(ma,max,false,true);
            Assert.assertEquals("selectivity of ("+ma+",Inf) incorrect!",cs,rs);
        }
    }

    @Test
    public void testNegativeInfinitySelectivityCorrect(){
        FloatColumnStatsCollector col = ColumnStatsCollectors.floatCollector(0,14,5);
        float[] v=loadData(col);

        FloatDistribution distribution=(FloatDistribution)col.build().getDistribution();
        float mi = Float.NEGATIVE_INFINITY;
        long s = distribution.selectivity(mi);
        Assert.assertEquals("Did not return 0 for nonexistent numbers!",0l,s);
        float min = distribution.min();
        for(int j=0;j<v.length;j++){
            float ma = v[j];
            long rs=distribution.rangeSelectivity(mi,ma,true,true);
            long cs = distribution.rangeSelectivity(min,ma,true,true);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,true,false);
            cs = distribution.rangeSelectivity(min,ma,true,false);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,false,true);
            cs = distribution.rangeSelectivity(min,ma,true,true);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);

            rs=distribution.rangeSelectivity(mi,ma,false,false);
            cs = distribution.rangeSelectivity(min,ma,true,false);
            Assert.assertEquals("selectivity of [-Inf,ma] incorrect!",cs,rs);
        }
    }

    @Test
    public void testGetPositiveCountForNegativeStartValues() throws Exception{
        FloatColumnStatsCollector col =ColumnStatsCollectors.floatCollector(0,14,5);
        for(int i=0;i<14;i++){
            col.update(0);
            col.update(1);
            col.update(-1);
            col.update(-Float.MAX_VALUE);
            col.update(Float.MAX_VALUE);
        }

        FloatColumnStatistics lcs = col.build();
        FloatDistribution distribution = new UniformFloatDistribution(lcs);

        long l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,false,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",14,l);

        l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,true,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",28,l);

        l=distribution.rangeSelectivity(-Float.MAX_VALUE,0,true,true);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",3*14l,l);
    }

    @Test
    public void distributionWorksWithFrequentElements() throws Exception {
 
    	FloatFrequencyCounter counter = FrequencyCounters.floatCounter(4);

        // Values repeated on purpose
        counter.update(101);
        counter.update(102);
        counter.update(102);
        counter.update(103);
        counter.update(103);
        counter.update(103);
        counter.update(104);
        counter.update(104);
        counter.update(104);
        counter.update(104);
        
		FloatFrequentElements fe =counter.frequentElements(4);

        FloatColumnStatistics colStats = new FloatColumnStatistics(0,
            CardinalityEstimators.hyperLogLogFloat(4),
            fe,
            101,
            104,
            200,
            12,
            0,
            2);

        UniformFloatDistribution dist = new UniformFloatDistribution(colStats);

        Assert.assertEquals(2,dist.selectivity(101)); // return min of 2, not actual 1
        Assert.assertEquals(2, dist.selectivity(102));
        Assert.assertEquals(3, dist.selectivity(103));
        Assert.assertEquals(4,dist.selectivity(104));
        Assert.assertEquals(0, dist.selectivity(105));
    }
	
    @Test
    public void testDistributionWorksWithSingleElement() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        FloatColumnStatistics scs = new FloatColumnStatistics(0,
                CardinalityEstimators.hyperLogLogFloat(4),
                FrequencyCounters.floatCounter(4).frequentElements(4),
                1,
                1,
                2,
                12,
                0,
                3);

        UniformFloatDistribution dist=new UniformFloatDistribution(scs);
        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(scs.minCount(),dist.selectivity(scs.min()));
        Assert.assertEquals(0l,dist.selectivity((scs.min()+1)));

        Assert.assertEquals(scs.minCount(),dist.rangeSelectivity(scs.min(),(scs.min()+1),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),false,true));
    }

    @Test
    public void emptyDistributionReturnsZeroForAllEstimates() throws Exception{
        //the test is to make sure that we can create the entity without it breaking
        FloatColumnStatistics scs = new FloatColumnStatistics(0,
                CardinalityEstimators.hyperLogLogFloat(4),
                FrequencyCounters.floatCounter(4).frequentElements(4),
                0,
                0,
                0,
                0,
                0,
                0);

        UniformFloatDistribution dist=new UniformFloatDistribution(scs);
        /*
         * We need to make sure we return 0 in the following scenarios:
         *
         * 1. values == scs.min()
         * 2. Values != min return 0
         * 3. Range estimates which include scs.min()
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(0,dist.selectivity(scs.min()));
        Assert.assertEquals(0l,dist.selectivity((scs.min()+1)));

        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(scs.min(),(scs.min()+1),false,true));
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private float[] loadData(FloatColumnStatsCollector col){
        FloatArrayList values = new FloatArrayList(1000);
        for(int i=0;i<100;i++){
            long il = 1l<<i;

            float v = il;
            col.update(v);
            values.add(v);
            v = -il;
            col.update(v);
            values.add(v);
            v = 1f/il;
            col.update(v);
            values.add(v);
            v = -1f/il;
            col.update(v);
            values.add(v);

            v = 3f*il;
            col.update(v);
            values.add(v);
            v = -3f*il;
            col.update(v);
            values.add(v);
            v = 3f/il;
            col.update(v);
            values.add(v);
            v = -3f/il;
            col.update(v);
            values.add(v);
        }

        values.add(-0f);
        values.add(0f);
        values.add(Float.MAX_VALUE);
        values.add(-Float.MAX_VALUE);
        values.add(Float.MIN_VALUE);
        values.add(-Float.MIN_VALUE);
        return values.toArray();
    }
}