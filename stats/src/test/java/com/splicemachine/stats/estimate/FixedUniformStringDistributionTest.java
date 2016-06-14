package com.splicemachine.stats.estimate;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.ComparableColumnStatistics;
import com.splicemachine.stats.cardinality.CardinalityEstimators;
import com.splicemachine.stats.collector.ColumnStatsCollector;
import com.splicemachine.stats.collector.ColumnStatsCollectors;
import com.splicemachine.stats.frequency.FrequencyCounter;
import com.splicemachine.stats.frequency.FrequencyCounters;
import com.splicemachine.stats.frequency.FrequentElements;
import com.splicemachine.utils.ComparableComparator;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

/**
 * Specific tests about the UniformStringDistribution
 *
 * @author Scott Fines
 *         Date: 6/25/15
 */
public class FixedUniformStringDistributionTest{


    @Test
    public void testUniformDistributionArbitraryData() throws Exception{
        /*
         * Regression test for DB-3729.
         *
         * The problem in DB-3729 is twofold. Firstly, values which are "far away" in the
         * unicode sense could result in wildly overestimating the uniform component of the
         * histogram. Secondly, values which are "close together" in the unicode sense could
         * dramatically underestimate the uniform component of the histogram. WHen adjustements
         * are then made for boundary conditions and frequent elements, we would end up with
         * either an estimation that was too high (higher than our totalCount()), or too low( an
         * estimation <0).
         *
         * This distills the reproducing case in the bug down to the barest example of the
         * problem. The data is generated from the reproducing case,and the tests merely check
         * that we don't exceed the bounds of the estimator (i.e. the resulting estimate must fall
         * in the range [0,values.length)).
         */
        String[] values={"A ", "I ", "Q ", "A ", "A ", "J ", "BEFORE ", "A ", "STAFF ", "TAB ",
                "T1 ", "S ", "B ", "SUMMER ", "A ", "P3 ", "WORKS ", "NT ", "B ", "A ",
                "EMPTY_TABLE_3 ", "TAB2 ", "NT ", "FUNCIONARIOS ", "A ", "E ", "TABLE2 ",
                "B ", "B4 ", "E ", "TABLE_DECIMAL_11_2 ", "LINEITEM ", "K ", "B ", "A ",
                "SYSCONGLOMERATES ", "SYSTABLES ", "SYSCOLUMNS ", "SYSSCHEMAS ", "SYSKEYS ", "SYSVIEWS ",
                "SYSCHECKS ", "SYSFILES ", "SYSDUMMY1 ", "SYSSTATEMENTS ", "SYSDEPENDS ", "SYSSTATISTICS ",
                "SYSCONSTRAINTS ", "SYSFOREIGNKEYS ", "CUSTOMER ", "ORDER_HEADER ", "WORKS ",
                "PERSON_ADDRESS ", "CATEGORY ", "DECIMALTABLE ", "D ",
                "TWO_NONCONTIGUOUS ", "TESTDROPUNIQUECONSTRAINT ", "DROPCOLPK ", "EMP_NAME ",
                "T8 ", "APOLLO_MV_MINUTE ", "COLLS ", "WORKS ", "PROJ ", "STAFF ", "UPUNIQ ", "PARENTT ",
                "NUMBERS ", "SMALLINTPK ", "DATEPK ", "TDELETE ", "A ", "P4 ", "A_TEST ", "EMPTY_TABLE_4 ",
                "T3 ", "SYSALIASES ", "SYSTRIGGERS ", "TNS_NOTNULLS ", "D ", "A ", "B ", "C ", "ORDERS ", "C ", "T1 ",
                "SYSTABLEPERMS ", "SYSUSERS ", "A ", "SYSSEQUENCES ", "SYSPERMS ", "SYSCOLPERMS ", "SYSROUTINEPERMS ",
                "SYSROLES ", "SYSPRIMARYKEYS ", "DC ", "F ", "N ", "OCC_TABLE ", "G ", "EMPTY_TABLE ", "ORDER_FACT ",
                "T4 ", "REGION2 ", "TERRITORIES ", "FOOD ", "T3 ", "T4 ", "ZONING1 ", "ZONING5 ", "ZONING6 ",
                "T1 ", "LOCATION ", "AA ", "SYSSTATEMENTHISTORY ", "ADDRESS ", "D ", "CHILDT ", "G ", "TS_MULTIPLEPK ",
                "E ", "G ", "CZAKHTACO ", "TNS_BOOL ", "D ", "T ", "TNS_DATETIME ", "SYSOPERATIONHISTORY ", "A ",
                "SYSTASKHISTORY ", "TS_NULLS ", "CUSTOMER ", "B ", "SYSBACKUP ", "SYSBACKUPITEMS ", "SYSBACKUPFILESET ",
                "SYSBACKUPJOBS ", "SYSTABLESTATS ", "SYSTABLESTATISTICS ", "STAFF ", "UPUNIQ ", "Z1 ", "WORKS ",
                "EMPTAB ", "C ", "TESTDROPCHECKCONSTRAINT ", "J ", "EMP_PRIV3 ", "ZONING3 ", "TAB ", "CHICKEN1 ",
                "PEOPLE ", "YEAR_VIEW ", "DD ", "NUMERICPK ", "M ", "G ", "J ", "WORKS8 ", "T2 ", "H ",
                "NULL_TABLE2 ", "ROLLBACK_TABLE ", "B ", "SYSCOLUMNSTATS ", "TS_FLOAT ", "A ", "VSUSUNPQF ",
                "TS_5_SPK ", "SYSCOLUMNSTATISTICS ", "SYSPHYSICALSTATS ", "B ", "PARTSUPP ", "ORYWMTYJU ",
                "A ", "A ", "OMSLOG ", "T ", "C ", "K ", "INCREMENT ",
                "TESTROLLBACKDROPUNIQUECONSTRAINT ", "TESTROLLBACKDROPPRIMARYKEY ", "S ", "EMPTAB ",
                "COLS ", "AB ", "FOO ", "CATEGORY_SUB ", "ORDER_LINE ", "F ", "Z2 ", "ITEM ", "TAB ", "B ", "D ",
                "E ", "F ", "A ", "A ", "A ", "B ", "SUPPLIER ", "A ", "EXPORT_LOCAL ", "TS_HIGH_CARDINALITY ",
                "SHIPMODE ", "PROJ ", "ZONING0 ", "THREE_CTG ", "TS ", "BEST_ADDR_FREQ ",
                "BIGINTPK ", "TIMESTAMPPK ", "EMP_NAME1 ", "STAFF ", "REVENUE0 ", "I ",
                "TAB4 ", "ZDCEGNMQZ ", "CATEGORY_SUB ", "ORDER_LINE ", "CUSTOMER1 ", "AB ", "ITEM ",
                "H ", "J ", "T ", "TNS_SINGLEPK ", "B ", "D ", "B ", "A ", "A ", "PART ", "A ", "DOCS ", "EMPPRIV ",
                "UU ", "J ", "H ", "P ", "T1 ", "TESTADDCOLUMN ", "TEST1 ", "B_PRIME ", "DUPES ", "B ", "C ", "L ",
                "NEW_ORDER ", "B3 ", "A ", "SORT_ON_NULL ", "NULL_TABLE ", "EXPORT_COMPRESSED ", "C ",
                "CUSTOMER ", "BB ", "G ", "L ", "A ", "DROPCOL ", "TNS_INT ", "A ", "TS_NONULLS ", "B ", "A ",
                "NATION ", "STAFF ", "UPUNIQ ", "AFTER ", "ZONING_08 ", "ERRORTABLE ", "EMAILABLE ",
                "SQLSTATISTICSIT_T2 ", "EMP_NAME_PRIV2 ", "ORDERSUMMARY ", "OMS_LOG ", "DOCS ", "CHICKEN3 ",
                "TXN_DETAIL ", "DT ", "VARCHARPK ", "TWO_CONTIGUOUS ", "TESTDROPPRIMARYKEY ", "T1 ", "TS_10_SPK ",
                "F ", "T4 ", "WORKS ", "T5 ", "EMPTY_TABLE ", "C ", "T ", "D ", "F ", "SHIPMENT ", "Y ", "A ", "B ",
                "TS_5_MPK ", "TS_CHAR ", "A ", "REGION ", "T2 ", "E ", "M ", "EMPTY_TABLE ",
                "TESTDROPUNIQUECONSTRAINTCREATEDWITH ", "DB_1315 ", "TEST2 ", "BETWEEN_TEST ", "A ", "ZONING_09 ",
                "RP_BC_14_1 ", "CUSTOMER ", "N ", "Q ", "TS_LOW_CARDINALITY ", "A ", "CUSTOMER ", "F ",
                "I ", "L ", "G ", "I ", "B ", "SAME_LENGTH ", "C ", "Z ", "D ", "A ", "A ",
                "PROJ ", "TABLE1 ", "THREE_OO_NCTG ", "EMP_PRIV2 ", "CHICKEN ", "ALL_SALES ", "CC ", "MONTHLY_HITS ",
                "ORDERS ", "DOUBLEPK ", "M ", "TASKS ", "E ", "A ", "R ", "T1 ", "ST_MARS ", "C ", "CATEGORY_SUB ",
                "ORDER_LINE ", "B ", "EXPORT_DECIMAL ", "BATCH_TEST ", "ITEM ", "E ", "CUSTOMER2 ", "FRED ", "TABS ",
                "FILES ", "WORKS ", "TNS_MULTIPLEPK ", "TNS_NULLS ", "A ", "TS_HIGH_CARDINALITY ", "ST2 ", "PROJ ",
                "B ", "J ", "R ", "ARITHMETIC ", "EMP_PRIV ", "TABLE2 ", "X ", "DD ", "H ", "T1 ", "ZONING2 ",
                "TABLE1 ", "F ", "ORDER_LINE ", "TABLE_SMALLINT ", "B ", "EMPNAME ", "PEOPLE ", "H ",
                "ST_EARTH ", "D ", "T1 ", "A ", "HMM ", "A ", "TNS_FLOAT ", "TS_NOTNULLS ", "A ", "UPUNIQ ", "STAFF ",
                "UPUNIQ ", "PROJ ", "STAFF ", "TWO_NONCONTIGUOUS_OUT_OF_ORDER ", "TESTDROPUNIQUECONSTRAINTTABLEHASPK ",
                "FOO ", "A1 ", "INTPK ", "TIMEPK ", "K ", "CONTENT ", "B2 ", "ZONING4 ", "TS_LOW_CARDINALITY ",
                "REAL_PRIVATE ", "B ", "B ", "CHAR_TABLE ", "T2 ", "TABLE2 ", "TABLE_INTEGER ",
                "F ", "TS_BOOL ", "PURCHASE ", "T1 ", "B ", "CUSTOMER ", "TS_10_MPK ", "AGRBDLYUZ ", "B ", "B ",
                "WARNING ", "TS_5_NPK ", "TS_DATETIME ", "A ", "A ", "A3 ", "A2 ", "G ", "O ",
                "NO_PK ", "NATION2 ", "EMPLOYEE_TERRITORIES ", "NT ", "PURCHASED ", "INCREMENT ", "E ",
                "EMP_PRIV ", "S ", "T4 ", "TS_LOW_CARDINALITY ", "CATEGORY ", "T5 ", "C ", "B ",
                "E ", "T ", "STYLE ", "CUSTOMER ", "ORDER_HEADER ", "T2 ", "A ", "B ", "F ",
                "TABLE_BIGINT ", "D ", "T2 ", "D ", "TDJXPJEWH ", "A ", "C ", "T1 ", "A ", "TS_HIGH_CARDINALITY ",
                "T2 ", "T3 ", "HITS ", "STAFF ", "UPUNIQ ", "SQLSTATISTICSIT_T1 ", "ALTERTABLEADDCOLUMN ",
                "EMP_NAME_PRIV ", "USER_GROUPS ", "CUSTOMERS ", "CHICKEN2 ", "DEPARTAMENTOS ", "BEST_IDS_POOL ",
                "EMPTAB_NULLS ", "CHARPK ", "B3 ", "B2 ", "H ", "T1 ", "R ", "PERSON ", "P ", "C ", "E ", "PIPE ", "S ",
                "F ", "H ", "CUSTOMER ", "ORDER_HEADER ", "A ", "T ", "WORKS ", "T1 ", "CONTENT_STYLE ",
                "TABLE_REAL ", "SHIPMENT ", "CATEGORY ", "DUPS ", "A ", "B ", "D ", "T2 ", "TNS_NONULLS ",
                "TS_HIGH_CARDINALITY ", "TWITHNULLS1 ", "D ", "L ", "TESTDROPUNIQUECONSTRAINTTABLEHASTWO ", "EMP ",
                "WORKS ", "PERSON ", "EMPTY ", "FOO2 ", "TEST ", "CHAR_DELETE ", "D ", "I ", "K ", "H ", "A ", "B ",
                "CHAR_TABLE_PS ", "A ", "D ", "E ", "KEYGEN ", "P1 ", "T1 ", "TABLE_DOUBLE ", "TASKS ", "EMPTY_TABLE_1 ",
                "T3 ", "ZONING7 ", "TNS_CHAR ", "TS_SINGLEPK ", "TABLE1 ", "T2 ", "WORDS ", "CUSTOMER ", "NEW_ORDER ",
                "OORDER ", "ORDER_LINE ", "THREE_NCTG ", "TBL ", "LINEITEM ", "REALPK ", "HEADER ", "TAB3 ",
                "TWITHNULLS2 ", "T ", "B ", "EMP ", "CC ", "PROJ ", "G ", "T ", "T2 ", "TAB ", "E ",
                "DATE_ADD_TEST ", "TAB3 ", "T ", "C ", "ST1 ", "G ", "PROJ ", "B ", "DUPLICATEVIEW ", "T1 ",
                "RATING ", "P2 ", "EXPORT_TEST ", "CUSTOMER_TEMP ", "TABLE_DECIMAL_5_0 ", "TS_INT ",
                "EMPTY_TABLE_2 ", "TS_10_NPK ", "TRUNCTEST ", "A ", "TS_LOW_CARDINALITY"};

        ColumnStatsCollector<String> col=ColumnStatsCollectors.collector(0,14,5,new DistributionFactory<String>(){
            @Override
            public Distribution<String> newDistribution(ColumnStatistics<String> statistics){
                return new UniformStringDistribution(statistics,Double.toString(-Double.MAX_VALUE).length());
            }
        });

        for(String val : values){
            col.update(val);
        }

        Arrays.sort(values);
        Distribution<String> distribution=col.build().getDistribution();
        for(int i=0;i<values.length;i++){
            String s = values[i];
            long count=distribution.selectivity(s);
            Assert.assertTrue("negative selectivity!",count>=0);
            Assert.assertTrue("overlarge selectivity!",count<=values.length);
            Assert.assertTrue("overlarge selectivity!",count<=distribution.totalCount());

            for(int j=i+1;j<values.length;j++){
                String t = values[j];
                if(t.equals(s)) continue;
                String m=t.compareTo(s)<0?t:s;
                String ma=t.compareTo(s)>0?t:s;

                long rs=distribution.rangeSelectivity(m,ma,true,true);
                Assert.assertTrue("negative selectivity: m=<"+m+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: m=<"+m+">, ma=<"+ma+">!:rs="+rs,rs<=values.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(m,ma,true,false);
                Assert.assertTrue("negative selectivity: m=<"+m+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: m=<"+m+">, ma=<"+ma+">!:rs="+rs,rs<=values.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(m,ma,false,true);
                Assert.assertTrue("negative selectivity: m=<"+m+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: m=<"+m+">, ma=<"+ma+">!:rs="+rs,rs<=values.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());

                rs=distribution.rangeSelectivity(m,ma,false,false);
                Assert.assertTrue("negative selectivity: m=<"+m+">, ma=<"+ma+">!rs="+rs,rs>=0);
                Assert.assertTrue("overlarge selectivity: m=<"+m+">, ma=<"+ma+">!:rs="+rs,rs<=values.length);
                Assert.assertTrue("overlarge selectivity!",rs<=distribution.totalCount());
            }
        }
    }

    @Test
    public void testGetPositiveCountForNegativeStartValues() throws Exception{
        ColumnStatsCollector<String> col=ColumnStatsCollectors.collector(0,14,5,new DistributionFactory<String>(){
            @Override
            public Distribution<String> newDistribution(ColumnStatistics<String> statistics){
                return new UniformStringDistribution(statistics,Double.toString(-Double.MAX_VALUE).length());
            }
        });

        for(int i=0;i<14;i++){
            col.update("0");
            col.update("1");
            col.update("-1");
            col.update(Double.toString(-Double.MAX_VALUE));
            col.update(Double.toString(Double.MAX_VALUE));
        }

        ColumnStatistics<String> lcs=col.build();
        Distribution<String> distribution=lcs.getDistribution();

        long l=distribution.rangeSelectivity("-1","0",false,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",14,l);

        l=distribution.rangeSelectivity("-1","0",true,false);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",28,l);

        l=distribution.rangeSelectivity("-1","0",true,true);
        Assert.assertTrue("Negative Selectivity!",l>=0);
        Assert.assertEquals("Incorrect selectivity!",3*14l,l);
    }

    @Test
    public void distributionWorksWithFrequentElements() throws Exception{
        // This test was added to cover issue in DB-3608

        FrequencyCounter<? super String> counter=
                FrequencyCounters.counter(ComparableComparator.<String>newComparator(),16,1);

        // Values repeated on purpose
        counter.update("A");
        counter.update("B");
        counter.update("B");
        counter.update("C");
        counter.update("C");
        counter.update("C");
        counter.update("D");
        counter.update("D");
        counter.update("D");
        counter.update("D");

        @SuppressWarnings("unchecked")
        FrequentElements<String> fe=(FrequentElements<String>)counter.frequentElements(5);

        // TODO: consider using ComparableColumn for stats integrity instead of
        // having to construct everything perfectly with this constructor.
        ColumnStatistics<String> colStats=new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogString(4),
                fe,
                "A",
                "D",
                200,
                12,
                0,
                2,
                UniformStringDistribution.factory(5));

        UniformStringDistribution dist=new UniformStringDistribution(colStats,5);

        Assert.assertEquals(2,dist.selectivity("A")); // return min of 2, not actual 1
        Assert.assertEquals(2,dist.selectivity("B"));
        Assert.assertEquals(3,dist.selectivity("C"));
        Assert.assertEquals(4,dist.selectivity("D"));
        Assert.assertEquals(0,dist.selectivity("Z"));
    }

    @Test
    public void distributionWorksWithASingleElement() throws Exception{
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),16,1);
        @SuppressWarnings("unchecked")
        FrequentElements<String> fe=(FrequentElements<String>)counter.frequentElements(1);
        ColumnStatistics<String> colStats=new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogString(4),
                fe,
                "hello",
                "hello",
                200,
                12,
                0,
                12,
                UniformStringDistribution.factory(5));

        //make sure the creation goes well
        UniformStringDistribution dist=new UniformStringDistribution(colStats,5);

        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(colStats.minCount(),dist.selectivity(colStats.minValue()));
        Assert.assertEquals(0l,dist.selectivity("hello1"));

        Assert.assertEquals(colStats.minCount(),dist.rangeSelectivity(colStats.minValue(),"hello1",true,true));
        Assert.assertEquals(0,dist.rangeSelectivity(colStats.minValue(),"hello2",false,true));
    }

    @Test
    public void distributionWorksForNoElements() throws Exception{
        FrequencyCounter<? super String> counter=FrequencyCounters.counter(ComparableComparator.<String>newComparator(),16,1);
        @SuppressWarnings("unchecked")
        FrequentElements<String> fe=(FrequentElements<String>)counter.frequentElements(1);
        ColumnStatistics<String> colStats=new ComparableColumnStatistics<>(0,
                CardinalityEstimators.hyperLogLogString(4),
                fe,
                null,
                null,
                0,
                0,
                0,
                0,
                UniformStringDistribution.factory(5));

        //make sure the creation goes well
        UniformStringDistribution dist=new UniformStringDistribution(colStats,5);

        /*
         * We need to make sure of the following things:
         *
         * 1. values == min or max return the correct count
         * 2. Values != min return 0
         * 3. Range estimates which include the min return minCount
         * 4. Range estimates which do not include the min return 0
         */
        Assert.assertEquals(0l,dist.selectivity("hello"));
        Assert.assertEquals(0l,dist.selectivity("hello1"));

        Assert.assertEquals(0l,dist.rangeSelectivity("hello","hello1",true,true));
        Assert.assertEquals(0,dist.rangeSelectivity("hello","hello2",false,true));
    }
}