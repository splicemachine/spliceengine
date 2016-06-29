package com.splicemachine.derby.impl.sql.execute.sequence;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SpliceSequenceIT {

    private static final String SCHEMA = SpliceSequenceIT.class.getSimpleName().toUpperCase();

    private static SpliceWatcher spliceClassWatcher=new SpliceWatcher();

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    private static SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher("FOO",SCHEMA,"(col1 bigint)");

    @ClassRule
    public static TestRule chain= RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher)
            .around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    try {
                        spliceClassWatcher.executeUpdate(format("insert into %s.%s values (1)", SCHEMA, "FOO"));
                    } catch (Exception e) {
                        throw new RuntimeException("Failure to insert");
                    }
                }
            });

    @Test
    public void testShortSequence() throws Exception {
        methodWatcher.executeUpdate("create sequence SMALLSEQ AS smallint");

        Integer first = methodWatcher.query("values (next value for SMALLSEQ)");

        assertEquals(first + 1, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 2, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 3, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 4, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 5, methodWatcher.query("values (next value for SMALLSEQ)"));
        assertEquals(first + 6, methodWatcher.query("values (next value for SMALLSEQ)"));

        assertTrue(first >= Short.MIN_VALUE && first <= Short.MAX_VALUE);

        methodWatcher.executeUpdate("drop sequence SMALLSEQ restrict");
    }

    @Test
    public void testSparkSequenceGenerationWithCreateAndDrops() throws Exception {
        for (int i=0;i<10;i++){
            methodWatcher.executeUpdate("create sequence FOOSEQ");
            Integer first = methodWatcher.query("select next value for FOOSEQ from foo");
            assertEquals(first + 1, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 2, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 3, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 4, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 5, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 6, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 1000, methodWatcher.query("select next value for FOOSEQ from foo --splice-properties useSpark=true"));
            // Spark by default allocates a 1K increment in the spark memory space
            // the next OLTP query will go to the table to get the sequence value which should start after the allocated entry...
            // from Spark.  Since Spark and Control run in different memory spaces, this is testing
            // whether they allocate their correct amounts...
            assertEquals(first + 7, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 8, methodWatcher.query("select next value for FOOSEQ from foo"));
            assertEquals(first + 9, methodWatcher.query("select next value for FOOSEQ from foo"));
            // Verifying the Spark Buffer is still available.  This test would not work if there are several spark nodes.
            assertEquals(first + 1001, methodWatcher.query("select next value for FOOSEQ from foo --splice-properties useSpark=true"));
            assertEquals(first + 1002, methodWatcher.query("select next value for FOOSEQ from foo --splice-properties useSpark=true"));
            methodWatcher.executeUpdate("drop sequence FOOSEQ restrict");
        }
    }

    @Test
    public void testSparkSequenceGenerationWithCreateAndDropsStartsWith10000 () throws Exception {
        for (int i=0;i<10;i++){
            methodWatcher.executeUpdate("create sequence FOOSEQ2 start with 10000");
            Integer first = methodWatcher.query("select next value for FOOSEQ2 from foo");
            assertEquals(first,new Integer(10000));
            assertEquals(first + 1, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 2, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 3, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 4, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 5, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 6, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 1000, methodWatcher.query("select next value for FOOSEQ2 from foo --splice-properties useSpark=true"));
            // Spark by default allocates a 1K increment in the spark memory space
            // the next OLTP query will go to the table to get the sequence value which should start after the allocated entry...
            // from Spark.  Since Spark and Control run in different memory spaces, this is testing
            // whether they allocate their correct amounts...
            assertEquals(first + 7, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 8, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            assertEquals(first + 9, methodWatcher.query("select next value for FOOSEQ2 from foo"));
            // Verifying the Spark Buffer is still available.  This test would not work if there are several spark nodes.
            assertEquals(first + 1001, methodWatcher.query("select next value for FOOSEQ2 from foo --splice-properties useSpark=true"));
            assertEquals(first + 1002, methodWatcher.query("select next value for FOOSEQ2 from foo --splice-properties useSpark=true"));
            methodWatcher.executeUpdate("drop sequence FOOSEQ2 restrict");
        }
    }

    @Test
    public void testSparkSequenceGenerationWithCreateAndDropsStartsWith10000IncrementsBy10 () throws Exception {
        for (int i=0;i<10;i++){
            methodWatcher.executeUpdate("create sequence FOOSEQ3 start with 10000 increment by 10");
            Integer first = methodWatcher.query("select next value for FOOSEQ3 from foo");
            assertEquals(first,new Integer(10000));
            assertEquals(first + 10, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 20, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 30, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 40, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 50, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 60, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 1000, methodWatcher.query("select next value for FOOSEQ3 from foo --splice-properties useSpark=true"));
            // Spark by default allocates a 1K increment in the spark memory space
            // the next OLTP query will go to the table to get the sequence value which should start after the allocated entry...
            // from Spark.  Since Spark and Control run in different memory spaces, this is testing
            // whether they allocate their correct amounts...
            assertEquals(first + 70, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 80, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            assertEquals(first + 90, methodWatcher.query("select next value for FOOSEQ3 from foo"));
            // Verifying the Spark Buffer is still available.  This test would not work if there are several spark nodes.
            assertEquals(first + 1010, methodWatcher.query("select next value for FOOSEQ3 from foo --splice-properties useSpark=true"));
            assertEquals(first + 1020, methodWatcher.query("select next value for FOOSEQ3 from foo --splice-properties useSpark=true"));
            methodWatcher.executeUpdate("drop sequence FOOSEQ3 restrict");
        }
    }


}