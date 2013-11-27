package com.splicemachine.derby.hbase;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import com.google.common.collect.Maps;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.test.framework.DefaultedSpliceWatcher;
import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;

@Ignore("Tests take FOREVER to run, so don't unignore them unless you REALLY want to")
public class SplittingTempTableIT extends SpliceUnitTest {

    private static final String SCHEMA_NAME = SplittingTempTableIT.class.getSimpleName().toUpperCase();

    @BeforeClass
    public static void setup() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        HTableDescriptor htd = admin.getTableDescriptor(SpliceConstants.TEMP_TABLE_BYTES);
        // This parameters cause the Temp regions to split more often
        htd.setMaxFileSize(10 * 1024 * 1024); // 10 MB
        htd.setMemStoreFlushSize(10 * 1024 * 1024); // 10 MB
        admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.modifyTable(SpliceConstants.TEMP_TABLE_BYTES, htd);
        admin.enableTable(SpliceConstants.TEMP_TABLE_BYTES);
        admin.close();
    }

//    @AfterClass
//    public static void cleanup() throws Exception {
//        HBaseAdmin admin = new HBaseAdmin(new Configuration());
//        HTableDescriptor htd = admin.getTableDescriptor(SpliceConstants.TEMP_TABLE_BYTES);
//        htd.remove(HTableDescriptor.MEMSTORE_FLUSHSIZE);
//        htd.remove(HTableDescriptor.MAX_FILESIZE);
//        admin.disableTable(SpliceConstants.TEMP_TABLE_BYTES);
//        admin.modifyTable(SpliceConstants.TEMP_TABLE_BYTES, htd);
//        admin.enableTable(SpliceConstants.TEMP_TABLE_BYTES);
//        admin.close();
//    }

    private static String TABLE_NAME_1 = "selfjoin";
    protected static DefaultedSpliceWatcher spliceClassWatcher = new DefaultedSpliceWatcher(
            SCHEMA_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(
            SCHEMA_NAME);
    protected static SpliceTableWatcher spliceTableWatcher1 = new SpliceTableWatcher(TABLE_NAME_1,
            SCHEMA_NAME, "(i int, j int,k varchar(10000),l varchar(10000),m varchar(10000))");

    private static final String LONG_STRING = "HAMLET: To be, or not to be--that is the question:\n" +
            "Whether 'tis nobler in the mind to suffer\n" +
            "The slings and arrows of outrageous fortune\n" +
            "Or to take arms against a sea of troubles\n" +
            "And by opposing end them. To die, to sleep--\n" +
            "No more--and by a sleep to say we end\n" +
            "The heartache, and the thousand natural shocks\n" +
            "That flesh is heir to. 'Tis a consummation\n" +
            "Devoutly to be wished. To die, to sleep--\n" +
            "To sleep--perchance to dream: ay, there's the rub,\n" +
            "For in that sleep of death what dreams may come\n" +
            "When we have shuffled off this mortal coil,\n" +
            "Must give us pause. There's the respect\n" +
            "That makes calamity of so long life.\n" +
            "For who would bear the whips and scorns of time,\n" +
            "Th' oppressor's wrong, the proud man's contumely\n" +
            "The pangs of despised love, the law's delay,\n" +
            "The insolence of office, and the spurns\n" +
            "That patient merit of th' unworthy takes,\n" +
            "When he himself might his quietus make\n" +
            "With a bare bodkin? Who would fardels bear,\n" +
            "To grunt and sweat under a weary life,\n" +
            "But that the dread of something after death,\n" +
            "The undiscovered country, from whose bourn\n" +
            "No traveller returns, puzzles the will,\n" +
            "And makes us rather bear those ills we have\n" +
            "Than fly to others that we know not of?\n" +
            "Thus conscience does make cowards of us all,\n" +
            "And thus the native hue of resolution\n" +
            "Is sicklied o'er with the pale cast of thought,\n" +
            "And enterprise of great pitch and moment\n" +
            "With this regard their currents turn awry\n" +
            "And lose the name of action. -- Soft you now,\n" +
            "The fair Ophelia! -- Nymph, in thy orisons\n" +
            "Be all my sins remembered.\n" +
            "\n" +
            "Read more at http://www.monologuearchive.com/s/shakespeare_001.html#5SGg36pHBiyOla5y.99";
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher1)
            .around(new SpliceDataWatcher() {

                @Override
                protected void starting(Description description) {
                    try {
                        PreparedStatement ps = spliceClassWatcher.prepareStatement(String.format(
                                "insert into %s (i, j,k,l,m) values (?,?,?,?,?)", TABLE_NAME_1));
                        for (int i = 0; i < 4; i++) {
                            ps.setInt(1, 1); ps.setInt(2, 1); ps.setString(3,LONG_STRING);ps.setString(4,LONG_STRING);ps.setString(5,LONG_STRING);
                            ps.executeUpdate();
                            ps.setInt(1, 1); ps.setInt(2, 3);ps.setString(3, LONG_STRING);ps.setString(4, LONG_STRING);ps.setString(5,LONG_STRING);
                            ps.executeUpdate();
                            ps.setInt(1, 2); ps.setInt(2, 2);ps.setString(3, LONG_STRING);ps.setString(4, LONG_STRING);ps.setString(5,LONG_STRING);
                            ps.executeUpdate();
                            ps.setInt(1, 2); ps.setInt(2, 4);ps.setString(3, LONG_STRING);ps.setString(4, LONG_STRING);ps.setString(5,LONG_STRING);
                            ps.executeUpdate();
                        }
                        /* 
                         * The table looks like this:
                         * 
                         *        i | j
                         *       --------
                         *        1 | 1
                         *        1 | 1
                         *        1 | 1
                         *        1 | 1
                         *        1 | 3 (x4)
                         *        2 | 2 (x4)
                         *        2 | 4 (x4)
                         */
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    } finally {
                        spliceClassWatcher.closeAll();
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new DefaultedSpliceWatcher(SCHEMA_NAME);

		@Test
		public void testRepeatedGroupedAggregate() throws Exception {
			for(int i=0;i<100;i++){
				testGroupAggregate();
					System.out.printf("iteration %d completed successfully%n ",i);
			}
		}

		@Test
//    @Ignore("Still breaking")
    public void testGroupAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select a.i, sum(a.j), avg(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE\n ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on d.i = e.i ",
                    "inner join " + TABLE_NAME_1 + " f --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on e.i = f.i ",
                    "inner join " + TABLE_NAME_1 + " g --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on f.i = g.i ",
                    "group by a.i"));
				int j = 0;
				Map<Integer,Pair<Integer,Integer>> corrects = Maps.newHashMap();
				corrects.put(1,Pair.newPair(4194304,2));
				corrects.put(2,Pair.newPair(6291456,3));
				while (rs.next()) {
						j++;
						int key = rs.getInt(1);
						Assert.assertFalse("Null key found!",rs.wasNull());
						Pair<Integer,Integer> correctValues = corrects.get(key);
						Assert.assertNotNull("key "+ key+" was not expected!",correctValues);
						int sum = rs.getInt(2);
						int avg = rs.getInt(3);
						Assert.assertEquals("Incorrect sum for key "+ key,correctValues.getFirst().intValue(),sum);
						Assert.assertEquals("Incorrect avg for key "+ key,correctValues.getSecond().intValue(),avg);
				}
				Assert.assertEquals("Did not see every row!",corrects.size(), j);
    }

    @Test
    public void testScalarAggregate() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select avg(cast (a.j as double)), sum(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on d.i = e.i ",
                    "inner join " + TABLE_NAME_1 + " f --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on e.i = f.i ",
                    "inner join " + TABLE_NAME_1 + " g --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on f.i = g.i "));
        final int sumFor1 = 4194304; // for a.i = 1 we have 4x1 and 4x3, self joined 6 times => (1+3)*4*(8**6) 
        final int sumFor2 = 6291456; // (2+4)*4*(8**6)
        boolean results = rs.next();
        Assert.assertTrue("No results", results);
        Assert.assertEquals("avg doesn't add up", 2.5, rs.getDouble(1), 0.0001);
        Assert.assertEquals("sum doesn't add up", sumFor1 + sumFor2, rs.getInt(2));
        Assert.assertFalse("More than one result", rs.next());
    }

    @Test
    @Ignore("Takes forever to run")
    public void testRepeatedScalarAggregate() throws Exception {
        for(int i=0;i<100;i++){
            testScalarAggregate();
            System.out.printf("Iteration %d succeeded%n",i);
        }
    }

    @Test
    @Ignore
    public void testRepeatedMergeSortSixJoins() throws Exception {
        for(int i=0;i<100;i++){
            testMergeSortJoinSixJoins();
            System.out.printf("Iteration %d succeeded%n",i);
        }
    }

    @Test
    public void testMergeSortJoinSixJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                        "on a.i = b.i "+
                                " inner join "+TABLE_NAME_1+" c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on b.i = c.i" +
                                " inner join "+TABLE_NAME_1+" d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on c.i = d.i" +
                                " inner join "+TABLE_NAME_1+" e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on d.i = e.i" +
                                " inner join "+TABLE_NAME_1+" f --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on e.i = f.i" +
        " inner join "+TABLE_NAME_1+" g --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                " on f.i = g.i"));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("Incorrect count",4194304,count);
        Assert.assertEquals("Incorrect sum",6291456,currentSumFor1);
    }

    @Test
    public void testMergeSortJoinFiveJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i,a.k,b.k,a.l,b.l from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                        "on a.i = b.i "+
                                " inner join "+TABLE_NAME_1+" c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on b.i = c.i" +
                        " inner join "+TABLE_NAME_1+" d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                        " on c.i = d.i" +
        " inner join "+TABLE_NAME_1+" e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                " on d.i = e.i" +
        " inner join "+TABLE_NAME_1+" f --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                " on e.i = f.i"));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("No results",524288,count);
        Assert.assertEquals("incorrect sum!",786432,currentSumFor1);
    }

		@Test
    public void testRepeatedMergeSortFiveJoins() throws Exception {
        for(int i=0;i<100;i++){
            testMergeSortJoinFiveJoins();
            System.out.printf("Iteration %d succeeded%n",i);
        }
    }

		@Test
		public void testRepeatedGroupedAggregateFiveJoins() throws Exception {
				for(int i=0;i<100;i++){
						testGroupAggregateFiveJoins();
						System.out.printf("Iteration %d succeeded%n",i);
				}
		}

		@Test
    public void testGroupAggregateFiveJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select a.i, sum(a.j), avg(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE\n ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on d.i = e.i ",
                    "inner join " + TABLE_NAME_1 + " f --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on e.i = f.i ",
                    "group by a.i"));
				int j = 0;
				Map<Integer,Pair<Integer,Integer>> corrects = Maps.newHashMap();
				corrects.put(1,Pair.newPair(524288,2));
				corrects.put(2,Pair.newPair(786432,3));
				while (rs.next()) {
						j++;
						int key = rs.getInt(1);
						Assert.assertFalse("Null key found!",rs.wasNull());
						Pair<Integer,Integer> correctValues = corrects.get(key);
						Assert.assertNotNull("key "+ key+" was not expected!",correctValues);
						int sum = rs.getInt(2);
						int avg = rs.getInt(3);
						Assert.assertEquals("Incorrect sum for key "+ key,correctValues.getFirst().intValue(),sum);
						Assert.assertEquals("Incorrect avg for key "+ key,correctValues.getSecond().intValue(),avg);
				}
				Assert.assertEquals("Did not see every row!",corrects.size(), j);
    }

		@Test
		public void testRepeatedGroupedAggregateFourJoins() throws Exception {
				for(int i=0;i<100;i++){
						testGroupAggregateFourJoins();
						System.out.printf("iteration %d correct%n",i);
				}
		}

		@Test
    public void testGroupAggregateFourJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select a.i, sum(a.j),avg(a.j) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE\n ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                    "on d.i = e.i ",
                    "group by a.i"));
        int j = 0;
				Map<Integer,Pair<Integer,Integer>> corrects = Maps.newHashMap();
				corrects.put(1,Pair.newPair(65536,2));
				corrects.put(2,Pair.newPair(98304,3));
        while (rs.next()) {
            j++;
						int key = rs.getInt(1);
						Assert.assertFalse("Null key found!",rs.wasNull());
						Pair<Integer,Integer> correctValues = corrects.get(key);
						Assert.assertNotNull("key "+ key+" was not expected!",correctValues);
						int sum = rs.getInt(2);
						int avg = rs.getInt(3);
						Assert.assertEquals("Incorrect sum for key "+ key,correctValues.getFirst().intValue(),sum);
						Assert.assertEquals("Incorrect avg for key "+ key,correctValues.getSecond().intValue(),avg);
        }
        Assert.assertEquals("Did not see every row!",corrects.size(), j);
    }

    @Test
    public void testMergeSortJoinFourJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i,a.k,b.k,a.l,b.l from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                        "on a.i = b.i "+
                                " inner join "+TABLE_NAME_1+" c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on b.i = c.i" +
                        " inner join "+TABLE_NAME_1+" d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                        " on c.i = d.i" +
        " inner join "+TABLE_NAME_1+" e --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                " on d.i = e.i"));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("Incorrect count",65536,count);
        Assert.assertEquals("incorrect sum!",98304,currentSumFor1);
    }

    @Test
    public void testMergeSortJoinThreeJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i,a.k,b.k,a.l,b.l,a.m,b.m from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                        "on a.i = b.i "+
                                " inner join "+TABLE_NAME_1+" c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                                " on b.i = c.i" +
                        " inner join "+TABLE_NAME_1+" d --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                        " on c.i = d.i"));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("No results",8192,count);
        Assert.assertEquals("incorrect sum!",12288,currentSumFor1);
    }

    @Test
    @Ignore("Ignored because it takes forever and ever to run, and generates some 300 regions")
    public void testRepeatedThreeJoinMSJ() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        for(int i=0;i<100;i++){
            List<HRegionInfo> tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            int numRegions = tableRegions.size();
            testMergeSortJoinThreeJoins();
            tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            System.out.println("iteration "+ i+ " successful. Temp space went from "+ numRegions+" to "+ tableRegions.size() +" during this run");
        }
    }

    @Test
    public void testMergeSortJoinTwoJoins() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i,a.k,b.k,a.l,b.l,a.m,b.m from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE \n",
                        "on a.i = b.i "+
                        " inner join "+TABLE_NAME_1+" c --DERBY-PROPERTIES joinStrategy=SORTMERGE \n"+
                        " on b.i = c.i"));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("Incorrect results",1024,count);
        Assert.assertEquals("incorrect sum!",1536,currentSumFor1);
    }

    @Test
    @Ignore("Ignored because it takes forever and ever to run, and generates lots of regions")
    public void testRepeatedTwoJoinMSJ() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        for(int i=0;i<100;i++){
            List<HRegionInfo> tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            int numRegions = tableRegions.size();
            testMergeSortJoinTwoJoins();
            tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            System.out.println("iteration "+ i+ " successful. Temp space went from "+ numRegions+" to "+ tableRegions.size() +" during this run");
        }
    }

    @Test
    public void testMergeSortJoinOneJoin() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                        "select a.i,a.k,b.k,a.l,b.l,a.m,b.m from ",
                        TABLE_NAME_1 + " a ",
                        "inner join " + TABLE_NAME_1 + " b --DERBY-PROPERTIES joinStrategy=SORTMERGE ",
                        "on a.i = b.i "));
        //manually add up and count records
        int currentSumFor1 =0;
        int count=0;
        while(rs.next()){
            int val = rs.getInt(1);
            Assert.assertFalse("Null record seen!",rs.wasNull());
            currentSumFor1+=val;
            count++;
        }

        Assert.assertEquals("No results",128,count);
        Assert.assertEquals("incorrect sum!",192,currentSumFor1);
//        Assert.assertEquals("avg doesn't add up", 2.5, rs.getDouble(1), 0.0001);
//        Assert.assertEquals("sum doesn't add up", sumFor1 + sumFor2, rs.getInt(2));
//        Assert.assertFalse("More than one result", rs.next());
    }

    @Test
    @Ignore("Takes forever to run")
    public void testRepeatedOneJoinMSJ() throws Exception {
        HBaseAdmin admin = new HBaseAdmin(new Configuration());
        for(int i=0;i<100;i++){
            List<HRegionInfo> tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            int numRegions = tableRegions.size();
            testMergeSortJoinOneJoin();
            tableRegions = admin.getTableRegions(SpliceConstants.TEMP_TABLE_BYTES);
            System.out.println("iteration "+ i+ " successful. Temp space went from "+ numRegions+" to "+ tableRegions.size() +" during this run");
        }
    }

    @Test
    public void testCountStar() throws Exception {
        ResultSet rs = methodWatcher.executeQuery(
                join(
                    "select count(*) from ",
                    TABLE_NAME_1 + " a ",
                    "inner join " + TABLE_NAME_1 + " b --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on a.i = b.i ",
                    "inner join " + TABLE_NAME_1 + " c --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on b.i = c.i ",
                    "inner join " + TABLE_NAME_1 + " d --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on c.i = d.i ",
                    "inner join " + TABLE_NAME_1 + " e --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on d.i = e.i ",
                    "inner join " + TABLE_NAME_1 + " f --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on e.i = f.i ",
                    "inner join " + TABLE_NAME_1 + " g --SPLICE-PROPERTIES joinStrategy=SORTMERGE ",
                    "on f.i = g.i "));
        final int totalRows = 4194304; // we start with 16 rows, self joined 6 times with half of the rows => 16*(8**6) 
        boolean results = rs.next();
        Assert.assertTrue("No results", results);
        Assert.assertEquals("Total rows don't add up", totalRows, rs.getInt(1));
        Assert.assertFalse("More than one result", rs.next());
    }

    private String join(String... strings) {
        StringBuilder sb = new StringBuilder();
        for (String s : strings) {
            sb.append(s).append('\n');
        }
        return sb.toString();
    }

}
